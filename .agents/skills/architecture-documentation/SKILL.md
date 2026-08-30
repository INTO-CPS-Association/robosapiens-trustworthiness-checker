---
name: architecture-documentation
description: Create or revise implementation-grounded architecture documentation using either a focused Rust module contract or a progressively disclosed project architecture. Use for Rust module docs, architecture landing pages, conceptual models, and focused mechanism pages that need precise facts, a stable example, rendered diagrams, executable evidence, and preservation-first anti-slop editing.
---

# Architecture documentation

Write architecture explanations that let readers form a correct mental model, predict important behavior, and descend into implementation detail only when they need it.

Use this skill for explanatory architecture documentation. Do not turn architecture pages into tutorials, API inventories, decision records, change recipes, or maintainer process guides.

## Load supporting guidance

Before drafting, read:

- [references/progressive-discovery.md](references/progressive-discovery.md) for abstraction levels, page roles, and depth limits;
- [references/architecture-pattern.md](references/architecture-pattern.md) for examples, traces, ownership, lifecycle, failure, and evidence;
- [references/unslop-checklist.md](references/unslop-checklist.md) before revising existing prose and during the final edit;
- [references/sources.md](references/sources.md) when attribution or the basis of this method matters.

Load the companion [architecture-diagrams](../architecture-diagrams/SKILL.md) skill when a relationship, lifecycle, boundary, temporal rule, or logical-to-physical mapping needs a figure. A diagram clarifies a verified architectural fact; it does not replace the prose contract or supply missing architecture.

## Choose the documentation surface first

Select one profile before gathering detail.

### Rust module documentation

Use the module profile for a coherent local semantic boundary exposed through Rust APIs. The primary readers are API users and maintainers reasoning about that module.

The document should establish:

1. the module's purpose and exact local scope;
2. the public semantic contract;
3. the principal public entities and the minimum internal owners needed to understand that contract;
4. special values, ordering, visibility, and persistence rules;
5. one concrete running example;
6. a trace when behavior spans calls, phases, or retained state;
7. public modes whose semantics differ;
8. a compact executable doctest when practical;
9. enough implementation orientation to locate representation and state;
10. lifecycle and failure consequences visible at the module boundary;
11. links to broader or deeper architecture documentation.

Do not reproduce the whole project architecture in `mod.rs`. Include deep representation, ownership, optimization, or literature material only when it is essential to understanding this module's contract or the module is itself the authoritative semantic reference.

Rustdoc does not use the mdBook Mermaid preprocessor. Use prose, tables, code, and committed custom SVG assets. Add a figure only when it explains a relationship more clearly than a compact example or trace.

### Project architecture documentation

Use the project profile for architecture landing pages and multi-page architecture areas. The primary readers are contributors and integrators discovering the system from purpose and boundaries toward mechanisms and code.

Apply progressive discovery:

```text
system purpose and external context
→ layer responsibility and scope of abstraction
→ organising concepts
→ principal entities and their responsibilities
→ one end-to-end scenario
→ subsystem contracts
→ focused ownership, lifecycle, failure, and resource mechanisms
→ implementation mapping
```

Do not collapse these levels into one landing page. A reader should be able to stop after any level with a coherent model.

The architecture landing page should state the system's responsibility, external actors or systems, principal inputs and outputs, major architectural regions, principal entities, cross-cutting invariants, and routes to deeper questions. It should not open with type names, field lists, cache sizes, phase functions, error variants, or optimization internals.

A conceptual-model page should establish the canonical behavior and reuse one end-to-end scenario. Focused pages should each answer one architectural question, such as who owns state, where a commit occurs, what replacement preserves, or where a failure becomes terminal. An implementation mapping may name source files, types, functions, tests, exact bounds, and physical layouts after the architectural model is established.

Read [references/progressive-discovery.md](references/progressive-discovery.md) for the complete page contracts.

## Gather evidence without exposing it all at once

Collect facts from implementation, tests, configuration, and existing documentation before drafting.

For module documentation, inspect public types and methods, constructors, state holders, phase functions, error types, doctests, and focused tests.

For project architecture, begin with system entry points, external boundaries, long-lived owners, data and control paths, lifecycle transitions, failure containment, deployment or runtime topology, and cross-subsystem invariants. Then trace those claims to concrete types, functions, tests, and configuration.

Record at least:

- responsibility and scope;
- inputs, outputs, and observable behavior;
- ordering and visibility rules;
- ownership and mutation points;
- lifecycle and reset points;
- failure scope and recovery boundary;
- resource growth or explicit bounds;
- canonical behavior and optional optimization;
- public modes or configurations that materially change semantics;
- uncertainty and claims that lack implementation evidence.

The author must know these facts, but every page must not state all of them. Assign each fact to the shallowest level at which a reader needs it. Link downward for the rest.

If implementation and prose conflict, investigate before rewriting. Report unresolved conflicts rather than selecting the more convenient account. Do not convert code shape into design intent.

## Build an explanatory spine

For a module or conceptual sequence, choose one small example that exposes the central distinction. Reuse its names and values in prose, traces, example-specific figures, and executable assertions.

For a project guide, choose one end-to-end scenario that crosses the major boundaries. Reuse it on the landing and conceptual pages where it helps readers retain orientation. Focused pages may use a small companion fixture when their question cannot be demonstrated clearly with the spine scenario. Label the companion fixture and do not pretend it is part of the original scenario.

Abstract context, ownership, containment, and failure diagrams do not need example identifiers. Their labels should name responsibilities and boundaries at the page's abstraction level.

## Draft module documentation from behavior inward

Use this progression when the subject supports it:

```text
local contract → principal entities → running example → trace → public modes
→ executable proof → representation orientation → state/lifecycle → failure
→ implementation mapping → deeper links
```

Omit steps that add no explanatory value. In particular, do not require an optimization section, literature comparison, ownership inventory, or diagram merely because the reference module contains one.

Keep implementation detail subordinate to the public model. Explain internal order, storage, or state only when it establishes a guarantee, prevents a likely misunderstanding, or gives maintainers the minimum orientation needed to locate the behavior.

## Draft project architecture through progressive discovery

Start each architecture area with purpose and boundaries, not implementation vocabulary. Introduce implementation names after the reader understands the responsibility they implement. Prefer “persistent evaluator state, implemented by `EvaluatorArena`” over beginning with `EvaluatorArena`.

Give each page one primary question. State the answer near the beginning, introduce the principal entities needed to follow it, then supply the scenario, diagram, contrasts, and implementation detail needed for that answer. End by linking to the next deeper question rather than recapping the page.

Keep page roles distinct:

- the landing page orients;
- the conceptual model defines canonical behavior;
- subsystem pages define contracts with adjacent regions;
- focused pages explain one mechanism or boundary;
- the implementation mapping connects established concepts to code and tests.

Do not repeat the full contract, scenario, component map, and failure model on every page. Repeat only the minimum invariant or terminology needed to make a page readable in isolation, then link to the canonical explanation.

## Establish what the layer abstracts

Before introducing entities or mechanisms, state the layer's responsibility and scope in domain terms. Explain what enters, what leaves, which variability the layer hides, and which concerns remain outside it. For an I/O layer, name the external forms it normalizes, the logical contract it presents inward, and the ownership, ordering, lifecycle, and failure semantics it abstracts. For an execution layer, define its computational model and the concepts that organize implementation before naming phases or optimizations.

Use a short **Scope of the abstraction**, **Layer responsibility**, or **Organising concepts** section when the opening paragraph cannot carry this clearly. A compact responsibility table is appropriate. Include explicit non-responsibilities where readers might otherwise assign work to the wrong layer—for example, transport composition does not create a distributed total order, and output admission does not prove remote persistence.

This section answers “what is this code for?” It must precede internal pipelines, detailed lifecycle, storage, and implementation mapping.

## Introduce principal entities before detailed behavior

After the opening contract and orientation figure, identify the small set of entities readers must recognize before the page descends into phases, state, routing, or failure. Use a compact table or list with two fields: **entity** and **responsibility**. Include external actors, long-lived owners, important immutable artifacts, and runtime coordinators only when they participate in the page's question.

Descriptions should be one or two sentences and answer what the entity owns, produces, coordinates, or guarantees. Introduce responsibility before or alongside the exact Rust name: “resolved input plan (`ResolvedInput`)” is clearer than an unexplained type name. Distinguish similarly named configured, resolved, opened, and session forms when their lifetimes differ.

Reuse those Rust names selectively in the detailed prose when they identify the actor, owner, artifact, or phase boundary responsible for a fact. Do not retreat to ambiguous “the monitor,” “the runtime,” or “the plan” when several such entities are in scope. Conversely, do not repeat a backticked type in every sentence once the referent is unambiguous. The entity introduction establishes vocabulary; the rest of the page should put that vocabulary to work.

This section is an architectural cast, not an API inventory. Do not list fields, constructors, helper types, every enum variant, or entities used only as implementation detail. A focused child page may introduce only the additional entities it expands and rely on linked parent context for the rest. In module documentation, introduce public semantic entities and the minimum internal owners required to understand the contract before tracing methods or phases.

## Show important public operations in code

After the page has established the abstraction, principal entities, and semantic contract, include a compact code example for each public operation that is central to using or understanding that contract. Examples are especially important for input construction and opening, output planning and delivery, direct evaluation, and reconfiguration when those operations define the page's subject.

An architecture example demonstrates a semantic boundary rather than generic setup. Show the smallest realistic transition from configured values to the public result: for example, source description → opened input stream, output destination → resolved plan → writer admission and flush, specification → repeated monitor ticks, or replacement specification → validated request. Keep transport credentials, CLI parsing, logging, and unrelated builder options out of the example.

Place examples only after readers know what the named types represent. Reuse the page's running-example names and values where practical, and state what the call proves and what it does not prove. In particular, distinguish pure planning from resource acquisition, admission from completion, structural request validation from runtime compilation, and one evaluation call from retained state across calls.

Prefer examples that:

- use public APIs exactly as exposed; never present crate-private phase methods as public entry points;
- use in-memory, null, or manual adapters so the semantic path is visible without external services;
- compile as doctests when practical and assert an architecturally meaningful result;
- use `no_run` only when execution inherently requires a long-lived runtime, external resource, credential, or environment not available to rustdoc;
- omit repetitive error plumbing with hidden doctest lines when that improves the public explanation without concealing a meaningful failure boundary.

Use the fence language that matches the code being shown. Standalone DSRV specifications use ```` ```dsrv ```` so the mdBook-local `theme/dsrv-highlight.js` grammar can distinguish declarations, types, built-ins, literals, and operators; do not label DSRV as `text` or `ocaml`. Keep DSRV source embedded in a Rust API example inside a `rust` fence, because the containing code is Rust.

A project architecture page is not an API inventory. Include only operations that cross or expose the architecture described by that page, and link to rustdoc for constructors and variants that do not change the reader's model. A focused failure, ownership, or layout page may need no API example when its question has no important public operation.

## Name the final map Implementation mapping

Use **Implementation mapping** for a section or dedicated page that connects established architectural concepts and entities to source files, types, functions, and focused tests. Mapping states where the architecture is realized; evidence remains the research basis for claims but is not the reader-facing section name.

Organize the mapping by concepts already introduced on the page. Do not present it as a repository tour, implementation procedure, or generic list of files.

## State architecture facts, not maintainer procedures

Architecture documentation tells maintainers what the implementation is and what must remain true. It does not tell them how to organize future work.

State:

- which component owns a value or transition;
- which order is observable;
- what persists, resets, commits, or becomes invalid;
- where a boundary lies;
- what errors are contained or terminal;
- which alternatives the current implementation exposes;
- which resource is fixed, bounded, cached, or unbounded;
- which implementation locations provide evidence.

Do not add:

- “when adding a new backend” or “when modifying this code” recipes;
- design-choice matrices for future changes;
- generalized implementation guidelines;
- review checklists inside architecture pages;
- speculative advice about extensions;
- recommendations unsupported by the current architecture.

A factual constraint is appropriate: “A schedule position is not a state identity.” A maintainer instruction is not: “When adding a scheduler, remember to use stable IDs.” Put operational procedures in a dedicated contributor or maintainer guide if the project needs them.

## Use numbered phase tables for long linear processes

When order is the main claim and the process is a single long chain, prefer a numbered phase table over Mermaid. Fullscreen rendering is not a reason to turn `A → B → C → …` into a diagram. A table keeps phase names readable at normal width and can state ownership, inputs, outputs, visibility, and failure consequences without putting prose inside nodes.

Use a factual table such as:

| # | Phase | Responsible entity | Consumes | Produces or changes |
|---:|---|---|---|---|

Choose only the columns the page needs. Number phases explicitly and reuse those numbers in nearby prose when referring to order. This is an execution or lifecycle description, not a decision table: it records what the current implementation does and does not advise maintainers which option to choose.

Retain a diagram when topology is the claim: branching, convergence, feedback, ownership regions, lifecycle transitions, temporal visibility, routing, concurrency, or failure containment. Use an exact custom SVG rather than a phase table when aligned logical ticks, simultaneous values, history cells, activation spans, destination projections, or pre/post-cutover regions are themselves the claim. A page can combine a large architecture overview, a numbered phase table for its linear spine, and focused diagrams for the non-linear or temporal relationships inside individual phases. Use a Mermaid `sequenceDiagram` when a focused interaction depends on caller/callee identity, returned plans or yielded items, awaited completion, acknowledgement, iteration across owners, or which effects precede a later failure. Sequence lifeline spacing is not logical time: retain talk-style SVGs for aligned ticks and phase tables for exhaustive serial work.

## Use diagrams at the reader's level

For mdBook architecture documentation, use fenced Mermaid when a standard family expresses the relationship clearly. Use the companion skill's custom SVG route for exact matrices, crossed mappings, talk-style logical-tick diagrams, fixed physical layouts, or figures shared with rustdoc.

Choose content before format:

- landing-page figures show context, responsibilities, and major boundaries;
- conceptual figures show canonical end-to-end behavior;
- subsystem overview figures show the subsystem's complete architectural path, adjacent owners, and principal internal responsibilities before individual mechanisms are introduced;
- abstract contract figures isolate one conceptual relationship without implementation layout;
- numbered phase tables show long linear execution, compilation, or replacement order;
- focused figures show one lifecycle, ownership, temporal, routing, or failure relationship;
- talk-style tick SVGs use one sparse shared baseline, regular tick marks, compact cells at exact x-positions, and thin activation spans to show simultaneity, history visibility, projection gaps, reduction, or cutover positions; serial phase internals remain in numbered tables;
- implementation figures may show IDs, slots, arenas, routing tables, and physical overlays.

### Layer diagrams on substantial architecture pages

A substantial landing, conceptual, or subsystem architecture page often needs more than one visual depth. After the opening answer and enough terminology to read it, place a large orientation figure that answers “how does this whole subject fit together?” The figure should expose the normal end-to-end path, major responsibility groups, adjacent systems, and the few cross-cutting alternatives or consequences needed to understand the rest of the page. It is an overview, not a compressed implementation map.

Descend from that overview through smaller figures only where the prose introduces a relationship that is difficult to retain textually:

```text
large orientation figure
→ numbered phase table for a long linear spine, when present
→ abstract contract or canonical-behavior figure
→ focused lifecycle, ownership, routing, temporal, or failure figure
→ exact custom SVG only where physical layout or alignment carries meaning
```

These figures have different questions and vocabulary depths. Do not make a focused figure by cropping the overview, and do not expand the overview until it contains every detail from later figures. Reuse stable entity names and preserve visible connection points so readers can locate each focused mechanism in the larger architecture.

A narrowly focused mechanism page normally inherits orientation from its parent page. Give it the minimum context and link upward rather than repeating the parent's large diagram. Add a page-level overview only when the page itself contains several responsibilities or phases that readers must orient before understanding its primary question.

Use theme-native Mermaid without diagram-local colors or styles. The mdBook theme supplies full-screen presentation centrally, so architecture pages contain only the semantic Mermaid source and its reading rule. Follow each figure with a reading rule that states the important relationship without relying on color. Do not force every page to contain Mermaid; one clear figure is better than format-driven duplication.

## Write exact prose

- Use one stable term for each concept.
- Name the responsible component when responsibility matters.
- Introduce responsibility before implementation name on higher-level pages.
- Put conditions beside the claims they constrain.
- Preserve modal strength: `must`, `may`, `never`, and `can` are not interchangeable.
- Distinguish guarantees, current implementation details, measurements, and inferred rationale.
- Give exact numbers, units, phase names, and error variants only at the level where they help the reader.
- Preserve useful repetition in traces, phase lists, lifecycle transitions, and deliberate contrasts.
- Make captions and reading rules explain direction, time, ownership, visibility, or containment rather than repeat titles.

## Revise in two passes

### Preservation pass

Compare the draft against the evidence inventory. Restore dropped conditions, caveats, bounds, ordering rules, failure distinctions, citations, and links. Verify terminology across prose, tables, code, figures, and related pages.

For a multi-page guide, also verify that each fact appears at the intended level and that deeper pages do not silently redefine the higher-level model.

### Unslop and depth pass

Remove throat-clearing, generic importance claims, fake previews, repeated summaries, inflated adjectives, vague actors, unsupported rationale, empty conclusions, and maintainer-guide drift.

Move details downward when they require concepts the page has not introduced. Remove details that belong only to code reference or contributor procedure. Do not flatten meaningful distinctions merely to shorten the document.

Use [references/unslop-checklist.md](references/unslop-checklist.md) as the final gate.

## Validate according to the surface

For Rust module documentation:

- run relevant doctests, normally `cargo test --doc` or a narrower supported target;
- build rustdoc with `cargo doc --no-deps` and inspect the rendered module page;
- verify intra-doc links and `include_str!` asset paths;
- check relevant feature combinations when documentation is feature-gated.

For mdBook architecture documentation:

- run the pinned Mermaid installer when generated browser assets are absent;
- run `mdbook build` through the repository's configured toolchain;
- inspect Mermaid in an actual browser at normal, narrow, and full-screen sizes under the default and preferred dark themes, including mouse-wheel zoom, drag panning, and zoom reset;
- verify that large overview and focused figures answer different questions, use consistent connection-point names, and appear in progressive depth order;
- verify page hierarchy, links, anchors, images, reading routes, and custom SVG rendering;
- confirm that the landing page can be understood without opening the implementation map.

For both surfaces:

- confirm factual claims against implementation, tests, configuration, or cited primary sources;
- run repository-required formatting and diff checks;
- report validation limitations accurately.

Do not report a check as passing unless it ran successfully.

## Deliver

Summarize:

- the selected profile and architectural question;
- the principal entities introduced and the facts mapped to implementation;
- how progressive depth or the local module contract was preserved;
- diagrams and executable evidence added;
- intentional omissions or unresolved uncertainty;
- validation commands and results.
