# Progressive discovery for architecture documentation

Progressive discovery gives readers a coherent answer at each depth and an obvious route to the next question. It does not mean withholding essential facts. It means introducing facts after the concepts needed to understand them.

## Project architecture depth

### System orientation

Answer:

- What responsibility does the project have?
- Which people, devices, services, files, networks, or runtimes interact with it?
- What enters and leaves?
- Which major regions divide responsibility?
- Which correctness, trust, timing, or failure properties cross those regions?

Use one large context or responsibility diagram near the top, after the opening responsibility statement and the minimum terminology needed to read it. It should orient the rest of the architecture area by showing external relationships, major responsibility groups, and the normal end-to-end direction. Label responsibilities in domain language. An implementation name may appear secondarily after the responsibility is clear.

Do not expose field lists, internal IDs, cache policies, exact error variants, phase functions, or physical execution tiers here.

### End-to-end conceptual model

Follow one representative input, command, request, or event through the major boundaries to its visible result. State what becomes observable at each boundary and what persists afterward.

This is the canonical home for the guide's spine scenario and high-level semantic contract. Use a trace only when time, ordering, or retained state is central to the project model.

Do not turn the scenario into an API tutorial. Omit setup details that do not explain architecture.

### Subsystem contracts

Begin by defining what the subsystem abstracts: the variability it accepts at its outer edge, the stable contract it presents inward, the responsibilities it owns, and the responsibilities it deliberately leaves to adjacent layers. Then explain its inputs and outputs, adjacent owners, state boundary, and failure boundary. State what the subsystem guarantees to its neighbors and what it assumes from them.

When the subsystem contains several phases, owners, delivery paths, or lifecycle regions, establish them first with a large subsystem overview diagram. Later sections may use abstract contract diagrams and focused mechanism diagrams to explain individual relationships visible in that overview. The overview remains at responsibility level; exact layouts and local transitions belong in the focused figures.

Name concrete implementations after the contract is understood. Link upward to project context and downward to focused mechanism pages.

### Focused mechanisms

Give one page to one question that requires deeper reasoning, such as:

- where state is owned;
- how one lifecycle transition occurs;
- when values become visible;
- where a failure is contained;
- what replacement preserves;
- how logical identities map to storage or execution;
- how canonical behavior survives optimization.

State the answer first. Add only the implementation detail needed to make that relationship verifiable.

### Implementation mapping

Connect established concepts and entities to source directories, files, types, functions, tests, configuration, exact bounds, and physical layouts. This level supports verification and code navigation.

Keep it factual. State where behavior is implemented and tested. Do not turn the map into instructions for future modifications or a checklist for code review.

## Page shape

A focused architecture page normally has:

1. a title naming the architectural question or subject;
2. a short answer or contract;
3. the minimum context inherited from a higher level;
4. a compact introduction to the principal entities and their responsibilities;
5. one scenario, contrast, or diagram that makes the relationship inspectable;
6. a compact example when an important public operation exposes the architecture;
7. an implementation mapping where code locations improve verification;
8. links upward and to the next deeper question.

Not every page needs every element. A page about ownership may need a diagram and object categories but no running trace. A failure page may need containment levels and state consequences but no public API example. Subsystem and conceptual pages should include examples for central public operations after their concepts and entities are introduced, without expanding into setup tutorials or API inventories.

A substantial page that spans several architectural relationships may instead use this visual sequence:

1. a large orientation diagram establishing the whole page scope;
2. a numbered phase table when the page has a long linear spine;
3. one or more abstract diagrams for canonical behavior or subsystem contracts;
4. focused diagrams beside the lifecycle, ownership, routing, temporal, or failure details they explain.

The sequence is progressive disclosure, not repetition. Each later figure answers a narrower question and keeps recognizable connection points from the overview. A focused child page should normally link to its parent's overview rather than reproduce it.

## Layer scope and organising concepts

Before the entity cast, define the layer in terms a reader can use without knowing the implementation. For I/O, state which source or destination differences disappear behind the abstraction and which ordering, ownership, backpressure, persistence, or transaction guarantees do not. For an evaluator, compiler, scheduler, or runtime layer, define the computational objects and relationships—such as graph nodes and edges, schedules, state owners, and accelerator overlays—that organize all later detail.

Link each organising concept to its canonical deeper page when one exists. The landing page should let a reader predict where a question belongs before they encounter source-file or type names.

## Entity introduction

Introduce the page's principal entities after its opening contract and orientation figure, before detailed phases or mechanisms. A compact entity/responsibility table works well when several owners or artifacts interact. Describe what each entity owns, coordinates, transforms, or retains; defer fields, methods, and helper types to API or implementation documentation.

At higher levels, use responsibility-first names and add concrete type names secondarily. At focused levels, introduce only entities that are new or whose role changes at that depth. Distinguish configuration, resolved plans, opened resource owners, and live sessions when the lifecycle depends on those forms.

The entity introduction should let a reader assign every later action to a known actor. Reuse the introduced Rust names where later ownership or phase statements would otherwise be ambiguous, while using ordinary nouns when the referent remains clear. The section should not become a complete component catalog or an isolated glossary that the rest of the page ignores.

## Diagram placement

Place the large orientation figure before detailed sections but after its essential vocabulary. It should make the section structure predictable: the major groups in the figure should correspond to concepts the page subsequently explains.

Place abstract and focused figures immediately after the prose states their question or contract. When a mechanism is a long single chain, use a numbered phase table instead and reserve figures for its branches, lifecycle, ownership, temporal, or failure relationships. Do not collect figures into a gallery, and do not put a detailed mechanism figure before readers can locate that mechanism in the page-level or parent-level overview.

A large diagram earns its size through architectural coverage, not through large labels or accumulated implementation detail. It may use fullscreen pan and zoom in the online book, but its primary reading order and major groups must remain recognizable at normal page width.

## Information placement

Place a fact at the shallowest level where it changes the reader's model.

- Project-wide responsibility and external boundaries belong on the landing page.
- Canonical observable behavior belongs in the conceptual model.
- Subsystem input/output and ownership contracts belong in subsystem overviews.
- Transition order, failure containment, and resource policy belong in focused pages.
- exact type names, function names, IDs, cache bounds, and tests belong in the implementation mapping or in focused verification paragraphs.

Repeat a higher-level invariant only when a lower-level page needs it to stand alone. Link to the canonical explanation instead of reproducing its full derivation.

## Vocabulary progression

Introduce domain responsibility before implementation vocabulary:

```text
persistent per-stream state, implemented by EvaluatorArena
```

not:

```text
EvaluatorArena is a Vec-like owner that...
```

After an implementation term has been introduced, use it consistently. Progressive discovery delays jargon; it does not replace exact terms with vague synonyms.

## Scenario scope

Use one spine scenario across orientation and conceptual pages when it represents the main flow. Reuse the same names and events so readers do not rebuild context.

Use a companion fixture on a focused page only when the spine scenario cannot expose that page's mechanism. State what the fixture isolates. Do not proliferate examples for visual variety.

## Architecture facts versus process guidance

Architecture pages describe the current system and its constraints:

- ownership;
- ordering;
- visibility;
- lifecycle;
- containment;
- resource policy;
- canonical and optional execution paths;
- implementation mapping.

They do not prescribe a workflow for changing the system. Avoid future-facing recipes, option-selection tables, review checklists, and general coding advice. Those belong in contributor or maintainer documentation when needed.

A page may document an actual runtime choice and its consequence. For example, a table comparing two supported execution modes is architecture. A table advising maintainers which design to implement for a future feature is not.

## Signs that a page is too deep

Move material downward when:

- internal names appear before their responsibilities;
- a reader must understand storage layout to learn what the system does;
- exact functions interrupt an end-to-end scenario;
- a landing page inventories modes instead of establishing boundaries;
- optimization appears before canonical semantics;
- implementation exceptions obscure the normal architectural path;
- the page answers several unrelated architectural questions.

Signs that a page is too shallow include vague component boxes, arrows meaning only “related,” failure described as generic propagation, ownership described as sharing, and lifecycle transitions omitted where state persists.
