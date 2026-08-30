# Architecture diagram families

Select the family that exposes the target relationship with the least visual machinery. In Zed Mermaid, prefer top-to-bottom compositions and theme-native defaults; use the repository SVG models as semantic references, not as a reason to copy their fixed colors.

## Architecture overview

**Use for:** the large orienting figure near the top of a landing, conceptual, or substantial subsystem page.

**Question:** how does the page's whole subject fit together from adjacent inputs through major responsibility groups to visible outputs or retained effects?

**Composition:** prefer `flowchart TB` with a dominant end-to-end spine and grouped responsibility regions. Show adjacent systems at the edges, normal flow through the center, and only the alternatives or cross-cutting consequences needed to understand later sections. Use stable connection-point labels that focused figures can repeat. Fullscreen pan and zoom support density, but the major groups and primary direction must remain recognizable at normal page width.

**Guardrail:** this is not an implementation inventory. Do not include every owner, stage, error, route, state transition, or optimization. If one region needs internal explanation, leave it as a named responsibility in the overview and expand it in a later focused figure.

## Short horizontal flow

**Use for:** a small pipeline, value transformation, function binding/call, or a single temporal feedback edge.

**Composition:** boxes left to right, one dominant arrow direction, optional dashed loop below the main path. In Mermaid, use `flowchart LR` only while the flow remains compact in a narrow pane.

**Repository models:** `example-streams.svg`, `function-binding.svg`, `function-call.svg`.

**Guardrail:** move to a vertical or grouped composition when more than about five to seven stages make labels cramped.

## Numbered phase table

**Preferred format:** Markdown table, not Mermaid, when a long process is a single ordered chain.

**Use for:** compilation phases, tick phases, request application, cutover order, or another process where sequence and per-phase facts are the claim.

**Composition:** number rows explicitly. Use columns such as phase, responsible Rust entity, consumes, produces or changes, visibility, and failure consequence. Keep the table factual; it is not a design-choice or maintainer decision table.

**Guardrail:** use a vertical Mermaid process only when side branches, convergence, containment, feedback, or temporal geometry add meaning that rows cannot express. Fullscreen zoom does not justify a diagram whose only information is top-to-bottom order.

## Grouped pipeline rows

**Use for:** compile-once versus run-many work, setup versus execution, or two mechanisms that converge on a common phase.

**Composition:** group panels with explicit headings; stages flow within each panel; a connector names the artifact crossing the boundary. Mermaid subgraphs inherit Zed's theme; do not add custom panel colors.

**Repository models:** `pipeline.svg`, `history-retention.svg`, `compilation-pipeline.svg`.

**Guardrail:** a panel boundary must correspond to a real lifecycle, ownership, or frequency boundary—not merely visual balance.

## Dependency graph plus snapshots

**Preferred format:** custom SVG when a matrix and aligned runtime snapshots appear together; Mermaid only for a small graph without exact comparative alignment.

**Use for:** potential versus active dependencies, permissions versus execution, or a graph whose edges vary by input.

**Composition:** stable graph or matrix first, followed by two or more runtime snapshots using the same node positions and labels.

**Repository model:** `dynamic-dependencies.svg`.

**Guardrail:** define inactive-edge styling explicitly. Repeated layouts should be geometrically identical so readers compare semantics, not positions.

## Talk-style tick diagram

**Preferred format:** custom SVG. Mermaid timelines, sequence diagrams, and state diagrams are suitable for coarse lifecycle transitions, but not when shared x-position is the semantic claim.

**Use for:** logical tick semantics, simultaneous values, history visibility, activation spans, input reduction, destination projection, or cutover positions. The project model is the sparse timeline grammar in `talk.pdf`: one shared time axis, short regular tick marks, compact value or update cells centered on exact positions, and thin spans showing which expression or configuration is active between positions.

**Composition:**

- draw one horizontal baseline shared by every row, with regular short tick marks and a small time arrow;
- put concise row labels to the left and center every event or value cell exactly over its logical position;
- use a compact cell, not a process box, for an update, value, control request, commit marker, or simultaneous tuple;
- use thin labeled horizontal spans above the baseline for activation or configuration lifetimes; end one span and begin the next at the exact replacement tick;
- preserve the same x-coordinate across source, result, projection, and history rows when they describe the same logical position;
- show omission as an empty position or gap, not a large “no event” box;
- use minimal arrows, normally only for an explicit transfer or mapping that position alone cannot express;
- place phase internals and long serial order in a numbered table outside the figure.

Values grouped in one cell are simultaneous. Separate cells are separate ticks. A physical flush or replacement interval is not another logical tick: label it on or between positions without distorting tick spacing.

**Repository models:** `two-tick-evaluation.svg`, `input-window-ticks.svg`, `output-routing-ticks.svg`, `dynamic-defer-ticks.svg`, `root-cutover-ticks.svg`, and the timeline portion of `context-transfer-ticks.svg`. `talk.pdf` is the visual model; its older claim that root reconfiguration is atomic is not an implementation model and must not be copied.

**Guardrail:** do not call a lane-based process diagram a tick diagram merely because its boxes align. Exclude architecture containers, phase cards, large explanatory boxes, dependency-flow arrows, and pipeline routing from this family. Distinguish physical batch boundaries from logical ticks, “computed at” from “visible at” and “committed at,” and a delivered control barrier from candidate activation. Current root cutover may drain admitted old work after the delivered barrier and may partially apply before failure.

## Participant interaction sequence

**Preferred format:** Mermaid `sequenceDiagram`.

**Use for:** request/response order, awaited calls, asynchronous handoff, admission versus completion, acknowledgements, per-owner iteration, and partial failure across named participants. A sequence diagram answers “who asks whom, what returns, and what may already have happened when a later message fails?”

**Composition:**

- order participants from the initiating boundary toward the final owner or external effect;
- label participants with the architecture term or exact Rust owner responsible for the interaction;
- use solid `->>` arrows for calls, deliveries, and ownership-bearing requests;
- use dashed `-->>` arrows for returns, yielded items, acknowledgements, and completion signals;
- use `loop` only for a real repeated set such as streams, destinations, or admitted items;
- use `alt` for mutually exclusive implementation branches and `opt` for conditional work;
- use activation bars only when a participant retains control across awaited nested interactions and the bar clarifies ownership;
- keep state mutation and visibility in message labels or nearby prose rather than inventing a state lifeline;
- begin and end at architectural boundaries already introduced by the page.

Follow the figure with a reading rule that defines call/delivery versus return/completion arrows and states the completion or failure consequence. Preserve conditions and bounds in prose when including them would turn messages into paragraphs.

**Guardrail:** do not use a sequence diagram for a long single-participant algorithm, an ordinary linear pipeline, dependency topology, stable ownership, or exact logical-tick alignment. Use a numbered phase table for serial phase facts, a flowchart for topology, a state diagram for legal lifecycle transitions, and a talk-style SVG when shared x-position carries temporal meaning. Do not infer calls from type dependencies, draw returns that the implementation does not await, imply concurrency from adjacent lifelines, or show rollback unless reversal is implemented.

## Activation or lifecycle timeline

**Preferred format:** Mermaid `stateDiagram-v2`, `sequenceDiagram`, or `timeline` for coarse transitions; custom SVG for dense comparative lifetimes that do not use the talk-style tick grammar.

**Use for:** owner lifecycle, retained-state lifetime, setup/active/retired transitions, or another temporal relationship where exact logical tick positions are not the primary claim.

**Composition:** name states and transitions directly. If several lanes are needed, align only verified causal or temporal boundaries and label non-tick intervals explicitly.

**Guardrail:** do not use this broader family as a substitute for a talk-style tick diagram when simultaneity, omission, activation at an exact tick, or historical visibility is the point.

## Mapping columns

**Preferred format:** custom SVG for crossed mappings or stable aligned indices; Mermaid for a small, uncrossed logical flow.

**Use for:** logical-to-physical layout, environment indices, routing, output projection, or crossed mappings.

**Composition:** aligned columns with headings; connectors map entries between columns; code-like identifiers use monospace.

**Repository models:** `environment-layout.svg`, `execution-layout.svg`.

**Guardrail:** crossings must demonstrate a meaningful reordering. Route and label them clearly; do not imply storage order is evaluation order.

## Nested containment

**Use for:** ownership scope, failure containment, I/O boundaries, or process/worker/instance hierarchy.

**Composition:** nested or stacked containers; shape and labels identify the focal scope; side labels state recoverability or ownership. Do not depend on an accent color to identify containment.

**Repository models:** `architecture-failure-ladder.svg`, ownership and I/O boundary figures.

**Guardrail:** containment means “inside this boundary,” not automatically “owned by.” Label ownership separately when both concepts appear.

## Canonical authority with optional overlay

**Preferred format:** Mermaid for a small guard/fallback flow; custom SVG when logical instructions must align with a fixed evaluator arena or per-node specialization state.

**Use for:** quickening, JIT, caches, specializations, fallback, or deoptimization that must preserve a canonical path.

**Composition:** canonical representation in the visually authoritative position; optional overlay below or beside it; guarded route into the overlay; explicit fallback/deoptimization arrow.

**Repository model:** `specialization-overlay.svg`.

**Guardrail:** do not draw the optimized path as replacing or mutating semantics unless the implementation does so. Name guards and invalidation boundaries.

## Multi-panel comparison

**Use for:** two implementations of one semantic rule, before/after reconfiguration, or static/dynamic variants.

**Composition:** panels share dimensions, vocabulary, scale, and reading order; a common footer or convergence stage shows what remains invariant.

**Repository models:** `history-retention.svg` and the runtime snapshots within `dynamic-dependencies.svg`.

**Guardrail:** vary only the property being compared. Cosmetic differences create false distinctions.

## Split criteria

Create multiple figures when any of these is true:

- the figure answers unrelated semantic questions;
- a single arrow style needs more than two meanings;
- there is no single reading order;
- labels require paragraph-length prose;
- more than four semantic colors are needed;
- a mobile-width rendering becomes illegible;
- implementation and lifecycle views need different coordinate systems.
