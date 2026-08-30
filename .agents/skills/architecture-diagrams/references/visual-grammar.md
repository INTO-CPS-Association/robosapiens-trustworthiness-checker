# Dataflow SVG visual grammar

Use these tokens only when the target requires a standalone SVG asset. For Markdown rendered in Zed, use [mermaid-style.md](mermaid-style.md) so the figure adapts to dark and light editor themes.

The existing SVG assets intentionally own a light canvas and exact colors. Match a nearby asset when local context establishes a more specific convention.

## Canvas

```text
background          #ffffff
canvas border       #d0d7de
canvas radius       6px
responsive sizing   width:100%; height:auto
wide max-width      usually 920–1030px
vertical max-width  usually 760–800px
```

Choose a `viewBox` whose dimensions match the composition. Do not stretch a dense horizontal composition into a narrow vertical slot.

## Text

```text
primary             #17212b
secondary           #52606d
document title      15px, weight 700, system-ui/sans-serif
section label       14px, weight 600, system-ui/sans-serif
artifact label      13px, weight 600, ui-monospace/monospace when code-like
description         12px, weight 400
tertiary note       11px
```

Use sentence case. Keep terminology identical to the prose and implementation. Prefer short noun or verb phrases; move qualifications into the caption or surrounding text.

## Neutral structure

```text
neutral panel fill  #f7f9fb
neutral panel line  #a8b3bf
arrow/connector     #65758b
normal line width   1.5
arrow line width    1.8
small box radius    5px
large group radius  7–8px
```

Use neutral grouped panels to organize dense diagrams instead of assigning every component a new color.

## Semantic palette

```text
blue fill/line      #e8f1fb / #2563a6
green fill/line     #e6f5f0 / #147d64
orange fill/line    #fff1e6 / #b54708
purple fill/line    #f3ebf8 / #8055a3
```

Assign color consistently within one figure. Possible roles include external/current input, canonical processing, retained state, and optional/optimized behavior, but no color has a repository-wide meaning independent of labels. State the meaning when it matters.

Use `stroke-width="2"` sparingly for the relationship or boundary the reader should notice first.

## Talk-style tick geometry

Use this geometry only for exact logical-time figures modeled on the timelines in `talk.pdf`:

```text
shared baseline       1.5px neutral line with a small terminal arrow
logical tick mark     8–12px vertical stroke crossing the baseline
row label             left aligned, 12–13px; monospace for Rust/DSRV names
value/update cell     compact, normally 36–110px wide and 28–42px high
activation span       2–3px horizontal rule aligned to exact start/end ticks
replacement boundary  adjoining labeled spans at one tick position
missing projection    empty x-position; optional tiny open marker or “∅” label
```

Keep tick positions regular even when labels differ. Every row that refers to a logical position uses the same x-coordinate. Group simultaneous values inside one compact cell rather than placing them sequentially. A physical flush, commit, control delivery, or replacement interval may sit between logical positions and must be labeled as such; it must not be drawn as an ordinary data tick.

The figure should remain sparse. Do not add large lane backgrounds, phase cards, architecture containers, or arrows between every row. Use arrows only for an explicit destructive transfer or mapping that cannot be represented by alignment. Put detailed ordered work in nearby prose or a numbered phase table.

## Arrows

A normal arrow marker uses:

```xml
<marker markerWidth="7" markerHeight="7" refX="9" refY="5"
        viewBox="0 0 10 10" orient="auto" markerUnits="strokeWidth">
  <path d="M 0 0 L 10 5 L 0 10 z" fill="#65758b" />
</marker>
```

Solid arrows normally denote active/current flow or normal forward execution. Dashed arrows can denote history, inactive permission, routing, fallback, or a post-phase relationship. Because those meanings differ, define dashed semantics per figure in the `<desc>`, legend, caption, or nearby prose.

Avoid bidirectional arrows unless both directions are separately meaningful. Prefer two labeled arrows when direction-specific behavior differs.

## Accessibility and collision safety

Every asset must include:

```xml
<svg role="img" aria-labelledby="figure-specific-title figure-specific-desc" ...>
  <title id="figure-specific-title">Concise subject</title>
  <desc id="figure-specific-desc">Reading order and important relationships.</desc>
</svg>
```

All IDs must be unique when assets are embedded inline on the same page. Prefix IDs with a stable figure slug. This includes marker, clip-path, mask, gradient, and filter IDs—not only title and description IDs.

Do not encode a distinction by color alone. Pair color with labels, geometry, line style, position, or a concise legend.

## Composition rules

- Use one obvious reading direction.
- Align repeated boxes and preserve equal padding.
- Keep connectors behind boxes and text where practical.
- Route lines around labels; avoid unexplained crossings.
- Put arrowheads outside destination text.
- Keep at least 12–16px internal padding for normal boxes.
- Use whitespace before adding separators.
- Split a figure before reducing normal labels below 11px.
- Avoid more than four semantic colors; many diagrams need only neutral plus one or two accents.

## Caption contract

The caption explains how to read the visual or why the relationship matters. Useful caption content includes:

- whether an edge crosses an execution boundary;
- when state becomes visible;
- whether layout order differs from projection order;
- what solid and dashed lines mean;
- which layer remains semantically authoritative;
- where failure becomes terminal or remains recoverable.

Do not duplicate the title as a caption.
