# Architecture explanation patterns

Use these patterns after selecting the module or project profile in `SKILL.md`. They describe ways to expose architectural facts; they are not a mandatory section template.

## Define the abstraction

State what variation the layer absorbs and what stable model it presents to adjacent layers. A useful scope statement names:

- outer forms or events accepted;
- inward semantic units produced;
- configuration and resource ownership hidden;
- ordering, lifecycle, and failure behavior owned;
- tempting but unsupported guarantees that remain outside the layer.

For computational layers, define the organising objects before the pipeline: graph, node, edge, schedule, state owner, canonical execution, and accelerator are examples. Link those concepts to the Rust entities and focused pages that realize them only after their meaning is clear.

## State a contract

A contract names one observable unit and its boundaries. Depending on the page, that unit may be a public call, tick, request, command, activation, generation, replacement, or process lifetime.

State:

- what enters;
- what completes successfully;
- what becomes visible;
- what persists afterward;
- what failure prevents or invalidates.

At project level, first state the responsibility and external boundary. At module level, state the local API semantics directly.

## Introduce the architectural cast

Before tracing detailed behavior, name the entities that perform or retain it. Use a compact entity/responsibility table or short list. Include only actors, owners, plans, state holders, and boundary artifacts needed for the page's question.

A useful description identifies lifecycle and responsibility together:

| Entity | Responsibility |
|---|---|
| resolved plan | immutable, resource-free description of validated ownership and routing |
| opened session | long-lived owner of resources and mutable live bindings |

Prefer responsibility-first wording at project level, followed by an exact type in parentheses when useful. This introduction is not an API index; omit convenience wrappers, helper structs, fields, and methods until a later implementation mapping requires them.

## Build a running example

Choose the smallest example that exposes the central distinction. A useful stateful example often contains:

- one external input;
- one current dependency;
- one retained or delayed value;
- one derived output or predicate;
- values short enough for a trace.

Write expected results before drafting prose. If order or history matters, include at least two units of execution.

Use one example through a module or conceptual sequence. A focused architecture page may use a separate fixture when it isolates a mechanism the spine example cannot show.

## Demonstrate the public semantic boundary

When an important public operation exposes the architecture, show it after its entities and contract are understood. Keep the example centered on one semantic transition:

- configured input source to opened logical batches;
- configured output destination to pure resolution, opened writer, admission, and completion barrier;
- compiled dataflow graph to repeated logical ticks and retained state;
- replacement specification and interface description to a structurally validated request.

Prefer in-memory, null, or manual adapters. Assert an architectural consequence such as preserved tick count, dependency-ordered output, or successful completion. State any stronger conclusion the example does not support: `send` is admission rather than remote persistence, and request validation is not replacement compilation or cutover.

Use only public entry points. If the implementation internally separates phases that are not public, explain those phases in prose while the code uses the public combined operation. Executable doctests are strongest for local deterministic behavior; reserve `no_run` for examples that genuinely require external resources or a long-running owner loop.

## Trace participant interactions

Use a Mermaid sequence diagram when architecture depends on interactions that a topology or phase table cannot express: a returned plan or drain, a yielded stream item, admission versus completion, an acknowledgement, iteration across stable owners, or partial effects before a later failure.

Name participants by responsibility and exact Rust owner where useful. Use solid arrows for calls and deliveries, dashed arrows for returns and completion, and only verified `alt`, `opt`, or `loop` regions. The surrounding prose remains authoritative for omitted conditions, bounds, and error variants.

Do not treat lifeline spacing as elapsed time or a logical tick axis. Keep exact simultaneity and commit positions in talk-style SVGs, legal owner transitions in state diagrams, topology in flowcharts, and exhaustive linear order in phase tables. A sequence diagram must add caller/callee or return semantics rather than redraw the same phase list horizontally.

## Trace temporal semantics

Use a trace when readers must compare current and retained values or distinguish computation from visibility and commit.

Useful columns include:

| Unit or phase | Reads | Produces | Visible now | Committed later |
|---|---|---|---|---|

State the reading rule beside the trace. Do not narrate the same row values again unless the prose adds a semantic distinction.

Keep these concepts separate when the implementation does:

- declaration order and dependency order;
- current state and committed history;
- potential dependency and active dependency;
- environment order and output projection order;
- configuration time and execution time;
- canonical evaluator and optimized execution overlay.

## Establish visual orientation before detail

On a substantial architecture page, use a large overview figure to establish the page's visual coordinate system before local mechanisms appear. It should answer one broad orientation question: how the page's complete subject moves, divides responsibility, or changes phase from its adjacent inputs to its visible outputs.

The overview identifies stable connection points and responsibility groups. Later abstract and focused figures reuse those names while changing scale:

- an abstract figure removes owners and physical layout to isolate the canonical contract;
- a focused figure expands one region to show ordering, state, routing, lifecycle, or failure;
- a custom SVG exposes exact geometry only when alignment, mapping, or repeated snapshots are semantic.

Do not force all three depths into one figure. Do not use several diagrams that restate the same pipeline with different decoration. The prose between figures should explain why the reader is changing scale.

## Summarize a pipeline

A numbered phase table should establish vocabulary for a long linear process. Use a diagram instead only when branching, feedback, ownership, containment, or temporal geometry is part of the claim:

| # | Phase | Responsible entity | Consumes | Produces | Becomes observable |
|---:|---|---|---|---|---|

Use implementation names only after the reader understands the phase responsibility. A project landing page normally needs major boundary crossings rather than a complete phase table.

## Explain ownership and layout

For each state category relevant to the page, determine:

- who allocates and owns it;
- who may mutate it;
- when mutation occurs;
- which identity points to it;
- when it is projected, committed, reset, replaced, or dropped;
- whether and how it grows.

State the ownership facts that answer the page's question. Keep exhaustive field and identity maps for focused or implementation-level pages.

When logical and physical order differ, name both and show the mapping. Never imply that an array position or schedule position is semantic unless the contract guarantees it.

## Explain lifecycle as transitions

Describe state through transitions rather than disconnected snapshots:

```text
created → configured → active → committed or reset → retired
```

Replace generic names with actual lifecycle states. Attach each visible effect and state change to its transition. State reconfiguration points and what remains valid across them.

## Classify failure

Research failure along these dimensions:

- origin;
- containment scope;
- recoverability;
- state consequence;
- next legal runtime state.

At higher levels, state containment boundaries and externally visible consequences. At focused and implementation levels, name exact variants and phase-specific behavior when they provide evidence.

Do not reduce failure to “errors are propagated.” Do not place an exhaustive error catalog on an architecture landing page.

## Bound resources

Determine whether collections, histories, caches, plans, queues, and evaluator instances are fixed, bounded, evicted, reset, or unbounded.

State resource policy where it changes architecture, lifecycle, or failure behavior. Give exact units and controlling configuration on focused or implementation pages. Avoid vague terms such as “small” or “limited.”

## Explain optimization as an overlay

Establish this order:

1. canonical semantics or representation;
2. equivalence required of optimized execution;
3. specialization inputs and guards;
4. cache or artifact ownership;
5. invalidation and fallback;
6. preserved error visibility.

Optimization belongs after the canonical model. A project landing page may state that optional executors preserve one contract; detailed guards and deoptimization belong deeper.

## Build an implementation mapping

After the architecture is established, map its existing concepts and entities to implementation locations. Group rows by architectural responsibility rather than source-tree order. A mapping may identify primary files, types, phase functions, and focused tests, but it should not explain how to modify them.

Call the reader-facing section **Implementation mapping**. Evidence is what the author used to establish the claims; the mapping is what helps a reader locate their realization.

## Use evidence appropriate to the claim

Strong evidence includes:

- public semantics: executable example plus public API and focused tests;
- ordering or visibility: trace plus phase implementation or test;
- ownership: type fields, constructors, and mutation sites;
- lifecycle: transition functions and reset or commit tests;
- failure: error types, propagation paths, and focused tests;
- bounds: constants or configuration plus enforcement sites;
- optimization equivalence: canonical fallback, guards, and differential tests;
- comparison to literature: primary papers or official technical sources.

Evidence belongs behind the architecture claim. Do not organize a project-level explanation around the order in which files or types were discovered.

Label inferred intent as inference. If motivation matters, establish it from an ADR, issue, design note, or cited source rather than deriving it from code shape.

## Keep architecture factual

Describe current responsibility, relationships, invariants, and consequences. Do not convert the evidence into generalized advice for future changes.

Appropriate:

> Schedule replacement changes execution order but leaves evaluator-owned state at stable stream identities.

Not architecture documentation:

> When implementing a new scheduler, make sure state is indexed by stable IDs.

The second statement may belong in a contributor guide, but it weakens an architecture page by changing from explanation to procedure.
