# SVG review checklist

## Semantic fidelity

- [ ] The figure answers one stated architectural question.
- [ ] Every entity and relationship is supported by code, tests, or established prose.
- [ ] Labels use the same terms as the implementation and surrounding documentation.
- [ ] Arrow direction matches execution, dependency, mapping, or time direction.
- [ ] Panel and containment boundaries have a real semantic meaning.
- [ ] Solid and dashed line meanings are explicit for this figure.
- [ ] Conditions, exceptions, and bounds omitted from the figure remain in prose.
- [ ] The diagram does not imply unsupported rationale or performance properties.

## Visual structure

- [ ] Reading order is immediately apparent.
- [ ] Repeated nodes, lanes, or snapshots align consistently.
- [ ] Text does not overlap boxes, connectors, or arrowheads.
- [ ] Connectors do not cross without a clear mapping reason.
- [ ] Font sizes follow the visual grammar and remain legible at narrow width.
- [ ] Color use is restrained and consistent within the figure.
- [ ] No distinction relies on color alone.
- [ ] Decorative elements do not compete with semantic ones.

## SVG integrity

- [ ] The root has a `viewBox`, `role="img"`, and responsive sizing.
- [ ] `aria-labelledby` references an existing `<title>` and `<desc>`.
- [ ] Title, description, marker, mask, clip-path, gradient, and filter IDs use a figure-specific prefix.
- [ ] IDs do not collide with other inline assets on the page.
- [ ] Every `url(#...)` and fragment reference resolves.
- [ ] The asset uses no scripts, remote resources, or `foreignObject`.
- [ ] The SVG parses as XML.

Example parsing command, when `xmllint` is available:

```sh
xmllint --noout path/to/figure.svg
```

For an asset set, inspect IDs with a repository-appropriate script or search. Do not assume IDs are isolated when documentation inlines SVG files.

## Accessibility text

- [ ] `<title>` names the subject concisely.
- [ ] `<desc>` gives the reading order and important relationships.
- [ ] `<desc>` includes temporal or line-style meaning needed without color.
- [ ] The documentation caption states the reading rule or consequence rather than repeating the title.
- [ ] Surrounding prose contains the complete technical contract.

## Render review

- [ ] The figure was opened or rendered with the documentation's actual toolchain when available.
- [ ] It was inspected at normal width.
- [ ] It was inspected at a narrow/mobile width.
- [ ] No text, border, path, or arrowhead is clipped.
- [ ] Browser or renderer differences do not hide essential content.
- [ ] The canvas dimensions suit its documentation placement.

Possible local tools include `rsvg-convert`, Inkscape, a browser, or the repository documentation renderer. Record which one was actually used.

## Documentation integration

- [ ] The figure path and reference are correct.
- [ ] The figure appears near the prose that establishes its terms.
- [ ] The caption and prose agree with the visual.
- [ ] Relevant link, docs, format, and diff checks pass.
- [ ] Validation limitations are reported explicitly.
