---
name: architecture-diagrams
description: Design or revise implementation-grounded architecture diagrams as Zed- and mdBook-rendered theme-native Mermaid or accessible custom SVG. Use for pipelines, dependencies, temporal semantics, ownership, lifecycle, failure containment, or logical-to-physical mappings that need precise labels, dark-background compatibility, captions, and repository-compatible rendering.
---

# Architecture diagrams

Create diagrams that answer one architectural question and remain correct when read with the surrounding prose. Use fenced Mermaid for Markdown rendered by Zed and `mdbook-mermaid` when a standard diagram family expresses the question clearly. Keep custom SVG as a first-class format for exact routing, dense comparative views, standalone assets, and `rustdoc` embedding. Do not substitute generated HTML, screenshots, or uneditable drawing formats.

A diagram is a compressed technical claim. Derive it from implementation evidence and documented semantics. Never invent missing components, order, causality, ownership, or rationale to make a picture look complete.

## Load supporting guidance

Before drawing, read:

- [references/rendering-strategy.md](references/rendering-strategy.md) to choose Mermaid or custom SVG and understand mdBook rendering;
- [references/mermaid-style.md](references/mermaid-style.md) for the dark-background-safe Zed/mdBook visual language;
- [references/diagram-families.md](references/diagram-families.md) to choose a composition;
- [references/mermaid-review-checklist.md](references/mermaid-review-checklist.md) before delivering Mermaid;
- [templates/architecture.mmd](templates/architecture.mmd) when starting a Mermaid flowchart, or [templates/sequence.mmd](templates/sequence.mmd) for participant interactions;
- [references/visual-grammar.md](references/visual-grammar.md), [references/svg-review-checklist.md](references/svg-review-checklist.md), and [templates/base.svg](templates/base.svg) only when producing SVG;
- [references/sources.md](references/sources.md) when attribution matters.

For substantial surrounding prose, load the companion [architecture-documentation](../architecture-documentation/SKILL.md) skill. The prose owns the full contract; the figure selects and clarifies one relationship.

## Gather evidence

1. Read the architecture prose and the code or tests that support it.
2. Write one sentence naming the question the figure will answer.
3. List entities, relationships, direction, temporal meaning, and boundary semantics.
4. Mark each relationship as verified, inferred, or unknown. Draw verified relationships only unless the caption explicitly labels an inference.
5. Identify the exact terms already used in public APIs and prose.
6. Decide what the caption must explain that geometry alone cannot.

If source prose and implementation disagree, stop and resolve or report the mismatch before drawing.

## Choose the output format

Use Mermaid when:

- the target is Markdown rendered in Zed and the online mdBook;
- a flowchart, sequence, state, class, ER, timeline, or other supported Mermaid family expresses the question without awkward routing;
- theme adaptation and maintainable text source matter more than exact coordinates.

Use custom SVG when:

- the same figure is inlined by `rustdoc` with `include_str!`;
- exact routing, repeated snapshots, matrices, crossed mappings, or dense timelines need manual coordinates;
- the target requires a standalone image asset;
- the figure needs explicit `<title>`, `<desc>`, and inline-SVG ID control;
- simplifying the composition for Mermaid would remove a semantic distinction.

`mdbook-mermaid` renders Mermaid to SVG in the browser; it does not create a committed `.svg` asset during `mdbook build`. Do not export Mermaid to a committed SVG merely to freeze Zed's appearance. Keep a custom SVG when a static asset is genuinely part of the architecture documentation contract.

## Choose one semantic question

Good figure questions include:

- What are the compile-time and run-time phases?
- Which value is read now versus after commit?
- Which component owns or mutates each state category?
- When does an activation start, commit, reset, or retire?
- Which participant initiates an interaction, what does another owner return or acknowledge, and what has completed before failure?
- How do logical outputs map to physical storage?
- Which failure boundary contains an error?
- Which dependencies are possible, and which are active in this execution?
- How does an optional specialization overlay fall back to canonical execution?

Do not draw a long linear `A → B → C → …` pipeline merely to show order. Move it to a numbered phase table unless branches, convergence, feedback, containment, or temporal geometry make the picture carry information beyond the sequence. Split the figure when its title needs “and” to join unrelated questions, when arrows need multiple unexplained meanings, or when labels become a prose substitute. A large orientation figure may cover several responsibility groups and phases, but its single question remains “how does this whole subject fit together?” It must not absorb the narrower ownership, lifecycle, routing, temporal, or failure questions of later figures.

## Design a progressive figure set

For a substantial architecture page, treat diagrams as a coordinated set rather than isolated illustrations. If the page has a long linear spine, place a numbered phase table between its orientation and focused figures rather than adding another Mermaid flow:

1. **Orientation overview:** the complete page scope, normal end-to-end direction, adjacent systems, major responsibility groups, and stable connection points.
2. **Abstract contract:** one canonical relationship with incidental owners, branches, and physical layout removed.
3. **Focused mechanism:** one overview region expanded to expose lifecycle, ownership, routing, time, failure, or resource semantics.
4. **Exact physical view:** a custom SVG only when aligned positions, crossed mappings, matrices, or repeated snapshots carry the claim.

Not every page needs every depth. Substantial landing, conceptual, and subsystem pages commonly need the overview followed by selected lower-level figures. A focused child page normally links to its parent's overview and draws only its local question.

Keep vocabulary and connection-point labels stable across depths. Change scale by omitting or expanding relationships, not by renaming the same entities. Each figure needs its own question and reading rule; the overview is not a thumbnail of all later diagrams, and focused figures are not decorative crops.

## Select a diagram family

Use [references/diagram-families.md](references/diagram-families.md):

- architecture overview for page-level orientation and major responsibility groups;
- horizontal flow for short pipelines and value movement;
- numbered phase table, outside Mermaid, for a long linear sequence where order is the main claim;
- vertical numbered flow only when side branches, containment, or spatial relationships make the sequence genuinely graphical;
- grouped panels for compile/run separation or mechanism comparison;
- graph plus snapshots for potential versus active relationships;
- talk-style tick diagram for exact logical positions, simultaneous values, activation spans, history visibility, projection gaps, and cutover positions;
- participant interaction sequence for calls, yielded items, awaited completion, acknowledgements, and partial failure across named owners;
- activation or lifecycle timeline for coarse temporal transitions that do not depend on exact logical tick alignment;
- mapping columns for logical-to-physical projection;
- nested containers for ownership or failure scope;
- authority/overlay stack for canonical and optimized execution.

Choose the family from the semantic question, not from visual novelty.

## Sketch semantics before coordinates

Prepare a text sketch first:

```text
Format: Mermaid or SVG, with reason
Title: one concrete claim or subject
Question: what the reader should know after viewing
Entities: exact labels
Solid arrows: meaning in this figure
Dashed arrows: meaning in this figure, if used
Groups: boundary represented by each panel
Reading order: left-to-right or top-to-bottom
Caption: interpretation or consequence not obvious from the title
```

Remove decorative nodes and arrows. Each remaining mark must encode an entity, relationship, boundary, state, sequence, or distinction supported by evidence.

## Build theme-native Mermaid for Zed

1. Start from [templates/architecture.mmd](templates/architecture.mmd) or the closest family in [references/mermaid-style.md](references/mermaid-style.md).
2. Put source in a fenced `mermaid` block so Zed renders it during authoring and `mdbook-mermaid` renders it in the published book. The book's central theme assets add the full-screen control; do not encode buttons, links, or presentation wrappers inside an individual diagram.
3. Omit `%%{init}%%`, custom themes, hardcoded fills or strokes, inline `style`, `linkStyle`, and `classDef`. Zed supplies theme-aware colors and an accent palette that remain readable on dark backgrounds.
4. Prefer `flowchart TB` for architecture. Use `LR` only for a short linear flow whose labels remain readable in a narrow pane.
5. Use subgraphs only for verified lifecycle, ownership, frequency, trust, or containment boundaries.
6. Keep stable Mermaid IDs short and semantic; put reader-facing text in quoted labels.
7. Use solid arrows for current flow or normal execution. Use dashed arrows for one explicitly stated alternate meaning, such as next-tick visibility or fallback.
8. Label condition, artifact, and boundary-crossing edges. Avoid unlabeled arrows that mean only “related.”
9. Carry semantics through labels, shapes, position, and line style so color is never required.
10. Follow the figure with a bold **Reading rule.** paragraph that defines edge styles and states the important consequence.

Use [references/mermaid-style.md](references/mermaid-style.md) as the complete grammar. Favor taller compositions: Zed and the published book may render into narrow panes, and wide diagrams force labels and nodes to shrink. Follow [references/rendering-strategy.md](references/rendering-strategy.md) when configuring or validating `mdbook-mermaid`.

## Build accessible SVG when required

For a talk-style tick diagram, preserve the project grammar before applying general SVG composition: use one sparse shared baseline, regular short tick marks, left-hand row labels, compact event/value cells centered exactly over ticks, and thin activation spans that change at exact positions. Empty projections remain visible gaps. Keep serial phase details in a numbered table; do not place phase cards, architecture containers, or process-flow arrows on the timeline. The visual reference is `talk.pdf`, but all semantics must be re-derived from the current implementation—especially root cutover, which is not atomic.

1. Copy [templates/base.svg](templates/base.svg) and replace every template identifier.
2. Set a `viewBox`; keep `width="100%"`, a suitable `max-width`, and `height:auto`.
3. Add `role="img"` and `aria-labelledby` referencing a unique `<title>` and `<desc>`.
4. Use unique IDs for the title, description, arrow markers, masks, clip paths, gradients, and filters. Multiple figures may be embedded inline on one page.
5. Use the repository tokens in [references/visual-grammar.md](references/visual-grammar.md).
6. Label every distinction. Never rely on color alone.
7. Define dashed-line meaning in the figure or caption. Dashed lines do not have a universal meaning across diagrams.
8. Keep text as SVG text, not paths. Use `system-ui, sans-serif` for prose labels and `ui-monospace, monospace` for code or artifact names.
9. Avoid `foreignObject`, external fonts, scripts, animations, and remote resources.
10. Keep labels clear of arrowheads and crossing lines. Increase the canvas or split the figure instead of shrinking text below the visual grammar's normal sizes.

Prefer simple SVG primitives and explicit coordinates. Do not add a build-time diagram dependency merely to produce one figure.

## Pair figure and caption

For Mermaid, precede the block with a descriptive heading or sentence and follow it with a **Reading rule.** paragraph. The surrounding text must provide the equivalent meaning for readers who do not inspect the rendered graphic.

For SVG, write alt semantics at three levels:

- `<title>`: concise subject;
- `<desc>`: reading order and the important relationships, including temporal or line-style meaning;
- documentation caption: the rule, contrast, or consequence the reader should retain.

A reading rule or caption should say, for example, that solid edges are active this tick while dashed edges are permitted but inactive. It should not merely repeat “Dynamic dependencies.”

Ensure the surrounding prose states any conditions, exceptions, and bounds omitted from the visual compression.

## Prevent diagram slop

Do not add:

- a generic “architecture” cloud;
- unlabeled databases, queues, gears, people, or service icons;
- gradients, shadows, 3D effects, or extra colors without semantic purpose;
- arrows that mean only “related to”;
- tiny prose paragraphs inside boxes;
- decorative section headings duplicated from nearby prose;
- unsupported claims of scalability, robustness, flexibility, or performance;
- implementation details unrelated to the figure's question.

Do not remove meaningful repetition such as phase numbers, current/committed labels, or repeated snapshots when comparison depends on it.

## Validate

For Mermaid, follow [references/mermaid-review-checklist.md](references/mermaid-review-checklist.md). For a multi-figure page, first verify that the overview and focused figures answer different questions, retain stable connection-point names, and appear in progressive depth order. Render every block in Zed and in the built mdBook at normal, narrow, and full-screen sizes. Inspect both the book's default theme and preferred dark theme for syntax, reading order, label wrapping, subgraph boundaries, arrow direction, contrast, the central full-screen control, mouse-wheel zoom, drag panning, zoom reset, and the reading-rule paragraph.

For SVG, follow [references/svg-review-checklist.md](references/svg-review-checklist.md): parse it as XML, check ID and fragment uniqueness, and inspect a real render at normal and narrow widths.

For either format, compare every label and relationship with code and prose, verify its documentation placement, and run repository-required formatting and diff checks. If the relevant renderer is unavailable, state that visual validation was not performed; source parsing alone does not prove layout quality.

## Deliver

Summarize:

- the question each figure answers;
- the evidence used;
- the chosen format and family, plus dashed-line semantics;
- theme and reading-rule checks for Mermaid, or accessibility and ID checks for SVG;
- render and repository validation results.
