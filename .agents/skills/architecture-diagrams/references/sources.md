# Sources and repository models

This skill is an original synthesis of the repository's existing diagrams, Zed's theme-aware Mermaid rendering conventions, Mermaid syntax, and public accessibility guidance.

## Primary repository models

The visual grammar comes from `docs/src/assets/dataflow/*.svg`, especially:

- `example-streams.svg` — short flow with temporal feedback;
- `pipeline.svg` — compile-once and per-tick phases;
- `environment-layout.svg` — logical-to-physical projection;
- `evaluation-graph.svg` — dependency direction;
- `history-retention.svg` — side-by-side mechanisms with common commit;
- `dynamic-dependencies.svg` — stable permissions and runtime snapshots;
- `dynamic-lifecycle.svg`, `dynamic-history.svg`, and `defer-lifecycle.svg` — temporal state;
- `specialization-overlay.svg` — canonical authority, optional optimization, and deoptimization;
- `execution-layout.svg` — logical plan and fixed evaluator layout;
- `architecture-overview.svg`, `architecture-static-tick.svg`, `architecture-dynamic-tick.svg`, and `architecture-failure-ladder.svg` — layered overview, vertical process, and failure containment.

`src/dataflow/mod.rs` supplies the surrounding prose and figure captions. Use both prose and implementation when deciding what a new figure may claim.

Repository files remain subject to the repository's license.

## Mermaid and Zed rendering

- [Mermaid flowchart syntax](https://mermaid.js.org/syntax/flowchart.html) — graph direction, subgraphs, shapes, links, and labels.
- [Mermaid diagram syntax](https://mermaid.js.org/intro/syntax-reference.html) — supported diagram families and source structure.
- [Mermaid theming](https://mermaid.js.org/config/theming.html) — background, text, line, and semantic colors are a renderer concern. The skill deliberately leaves these to Zed or the mdBook initializer rather than embedding an `init` configuration.
- [`mdbook-mermaid`](https://github.com/badboy/mdbook-mermaid) — mdBook preprocessor and local browser assets for rendering fenced Mermaid blocks. This repository pins `0.16.2` for compatibility with mdBook 0.4.

Zed renders fenced `mermaid` blocks using the active editor theme and accent palette. `mdbook-mermaid` preprocesses the same blocks and Mermaid renders them to SVG in the published browser page. The skill therefore forbids diagram-local colors and style directives so one source remains readable on dark and light backgrounds.

## Public accessibility guidance

- [WAI: Images Tutorial](https://www.w3.org/WAI/tutorials/images/) — choosing useful text alternatives and ensuring surrounding content carries equivalent information.
- [WAI-ARIA: `aria-labelledby`](https://www.w3.org/WAI/ARIA/apg/practices/names-and-descriptions/#labelledby) — accessible naming by reference.
- [MDN: SVG `<title>` element](https://developer.mozilla.org/en-US/docs/Web/SVG/Reference/Element/title) — short accessible text for SVG content.
- [MDN: SVG `<desc>` element](https://developer.mozilla.org/en-US/docs/Web/SVG/Reference/Element/desc) — detailed textual description of an SVG.
- [WCAG: Use of Color](https://www.w3.org/WAI/WCAG22/Understanding/use-of-color.html) — do not make color the only means of conveying information.

Links were reviewed during skill design in August 2026. Recheck external guidance if standards or documentation tooling change.
