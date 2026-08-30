# Mermaid review checklist

## Semantic fidelity

- [ ] The figure's role is explicit: orientation overview, abstract contract, focused mechanism, or exact physical view.
- [ ] A long single-chain process was moved to a numbered phase table unless the figure adds branching, convergence, feedback, containment, or temporal geometry.
- [ ] The figure answers one stated architecture question.
- [ ] Every node, boundary, and edge is supported by code, tests, or established prose.
- [ ] Labels use the same terms and running-example identifiers as the documentation.
- [ ] Arrow direction matches execution, dependency, mapping, ownership, or time direction.
- [ ] Each subgraph represents a verified lifecycle, ownership, frequency, trust, or containment boundary.
- [ ] Solid and dashed edge meanings are explicit and consistent within the figure.
- [ ] Conditions, exceptions, error variants, and bounds omitted from the figure remain in prose.

## Zed and mdBook theme compatibility

- [ ] The block contains no `%%{init}%%` directive or custom theme.
- [ ] It contains no hardcoded colors, `style`, `linkStyle`, or `classDef`.
- [ ] Meaning does not depend on fill, stroke, or text color.
- [ ] Labels, shapes, topology, and line style preserve every important distinction.
- [ ] The diagram was rendered against a dark Zed background.
- [ ] The built mdBook was inspected with its default and preferred dark themes.
- [ ] Theme accents remain semantic-neutral: changing the Zed or mdBook theme does not change the claim.

## Layout and syntax

- [ ] The source is inside a fenced `mermaid` block.
- [ ] `flowchart TB` is used unless a horizontal or different diagram family fits better.
- [ ] Stable IDs are short and semantic; reader-facing labels are quoted.
- [ ] Node labels remain short and readable.
- [ ] Edge labels name conditions, artifacts, or temporal boundaries.
- [ ] The diagram has one obvious reading order.
- [ ] There are no unexplained edge crossings or bidirectional arrows.
- [ ] In a sequence diagram, every participant is explicit and ordered from initiating boundary toward owner or effect.
- [ ] In a sequence diagram, solid arrows are calls/deliveries and dashed arrows are returns/completions; the reading rule states that convention.
- [ ] Every `alt`, `opt`, and `loop` corresponds to a verified branch, condition, or repeated owner/item set.
- [ ] Lifeline spacing is not presented as elapsed or logical-tick time, and no unimplemented acknowledgement or rollback is drawn.
- [ ] The source uses no inline HTML elements.
- [ ] Zed and `mdbook-mermaid` render the block without a Mermaid syntax error.

## Render review

- [ ] A large overview exposes its major groups and primary reading direction at normal width before fullscreen inspection.
- [ ] The rendered diagram was inspected at normal pane width.
- [ ] It was inspected at narrow pane width.
- [ ] It was inspected fullscreen, including mouse-wheel zoom, primary- and middle-button drag panning, and reset.
- [ ] Fullscreen zoom changes the SVG `viewBox` rather than applying a rasterizing CSS transform.
- [ ] Text and strokes remain sharp at several zoom levels and are not clipped.
- [ ] Subgraph labels and boundaries remain clear.
- [ ] Edge labels do not collide with nodes or arrowheads.
- [ ] The figure is taller rather than excessively wide where practical.
- [ ] Dense content was split instead of compressed.

## Documentation integration

- [ ] On a multi-figure page, diagrams descend from orientation to abstract contract to focused mechanism without duplicating one another.
- [ ] Stable entity and connection-point names are reused across diagram depths.
- [ ] A focused child page links to its parent overview instead of repeating it unless the child has a distinct multi-region scope.
- [ ] The prose introduces the question before the figure.
- [ ] A bold **Reading rule.** paragraph follows the figure.
- [ ] The reading rule defines dashed edges without referring to colors.
- [ ] The prose carries an equivalent semantic explanation for readers who cannot inspect the graphic.
- [ ] The figure appears after its terminology and running example are established.
- [ ] Relevant documentation, formatting, and diff checks pass.
- [ ] Any inability to render in Zed or the built mdBook is reported explicitly.
