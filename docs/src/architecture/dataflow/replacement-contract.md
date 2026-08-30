# Replacement identity

State survives replacement only when the target can prove semantic compatibility with an active owner. Schedule positions, physical route positions, and coincidental storage indices are not semantic identities.

![Stable environment slots and output projections remain distinct from evaluation order](../../assets/dataflow/environment-layout.svg)

**Reading rule.** Stable stream and environment identities select state and row locations. Scheduler and output projections can order those identities differently without redefining them.

## Identity domains

| Identity | Scope |
|---|---|
| `DefinitionKey` | complete compiled monitor definition |
| `StreamStateKey` | state-bearing semantics of one computed stream |
| `ExpressionStateKey` | one nested reconfiguration occurrence |
| `StreamId` | logical computed stream within one monitor |
| `EnvironmentSlot` | current-row location within one definition |
| `NodeId` | operation and matching state slot within one graph |
| `PlanId` | schedule-specific physical route |
| `MonitorRevision` | accepted root or nested semantic activations |
| `InterfaceRevision` | effective input/output interface activations |

`PlanId` is deliberately outside the semantic state domains. A route can be replaced while all evaluator identities persist.

## Cross-definition mapping

`ReconfigurationMapping::between` is indexed by the target definition. A computed stream maps when variable identity and `StreamStateKey` agree. Environment values map by variable identity and compatible type.

This permits stream reordering without state movement. Added or semantically changed target streams remain unmapped and start cold. Removed source state has no target owner and is dropped. Mapping validation checks target completeness, source bounds, source injectivity, names, and state keys before destructive transfer begins.

## Root plan selection

An exact healthy definition can retain the active monitor. A changed definition with transfer enabled prepares a target and mapping. Transfer disabled, failed donor state, or incompatible semantics installs cold state.

Accepted root application advances `MonitorRevision`, even when exact state is retained. `InterfaceRevision` advances only when the resolved input/output interface changed. Revision overflow is an error.

## Nested identity

A nested body belongs to one `ExpressionStateKey`. Equal source text to the currently active body keeps that evaluator. A changed body may receive compatible state from the immediately previous evaluator. Returning later to old text creates a new activation; source text is not a historical evaluator key.

## Implementation mapping

Identity keys and revisions are defined in `src/dataflow/reconfiguration.rs`; target-indexed mapping is in `src/dataflow/reconfiguration_mapping.rs`; complete definition keys are produced by `src/dataflow/program.rs`. Focused tests cover reordered streams, exact matches, changed streams, mapping validation, domain separation, and revision overflow.

Continue with [context transfer](context-transfer.md) for state movement and [execution tiers](execution-tiers.md) for replaceable routes.
