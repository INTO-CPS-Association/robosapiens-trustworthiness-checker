#!/usr/bin/env python3
"""Generate the SVG assets included by the dataflow module documentation.

Run without arguments to update the SVG assets. Use --check in CI or before
committing to verify that generated output is current.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ASSET_DIR = ROOT / "docs/src/assets/dataflow"

COLORS = {
    "ink": "#17212b",
    "muted": "#52606d",
    "line": "#65758b",
    "border": "#a8b3bf",
    "panel": "#f7f9fb",
    "blue": "#2563a6",
    "blue_fill": "#e8f1fb",
    "green": "#147d64",
    "green_fill": "#e6f5f0",
    "orange": "#b54708",
    "orange_fill": "#fff1e6",
    "purple": "#8055a3",
    "purple_fill": "#f3ebf8",
}


def esc(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def text(x: int, y: int, value: str, *, anchor: str = "middle", cls: str = "label") -> str:
    styles = {
        "label": ("system-ui, sans-serif", 14, 600, COLORS["ink"]),
        "code": ("ui-monospace, monospace", 13, 600, COLORS["ink"]),
        "small": ("system-ui, sans-serif", 12, 400, COLORS["muted"]),
        "section": ("system-ui, sans-serif", 15, 700, COLORS["ink"]),
    }
    family, size, weight, fill = styles[cls]
    return (
        f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-family="{family}" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}">{esc(value)}</text>'
    )


def box(
    x: int,
    y: int,
    width: int,
    height: int,
    label: str,
    *,
    fill: str = "panel",
    stroke: str = "border",
    sublabel: str | None = None,
) -> str:
    center = x + width // 2
    label_y = y + (height // 2 if sublabel is None else height // 2 - 7) + 5
    parts = [
        f'<rect x="{x}" y="{y}" width="{width}" height="{height}" rx="5" '
        f'fill="{COLORS[fill]}" stroke="{COLORS[stroke]}" stroke-width="1.5"/>',
        text(center, label_y, label, cls="code"),
    ]
    if sublabel is not None:
        parts.append(text(center, label_y + 18, sublabel, cls="small"))
    return "\n".join(parts)


def arrow(x1: int, y1: int, x2: int, y2: int, marker: str, *, dashed: bool = False) -> str:
    dash = ' stroke-dasharray="5 4"' if dashed else ""
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
        f'stroke="{COLORS["line"]}" stroke-width="1.8" marker-end="url(#{marker})"{dash}/>'
    )


def svg(title: str, description: str, width: int, height: int, body: str, marker: str) -> str:
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-labelledby="{marker}-title {marker}-desc" style="width:100%;max-width:{width}px;height:auto;background:#ffffff;border:1px solid #d0d7de;border-radius:6px">
<title id="{marker}-title">{esc(title)}</title>
<desc id="{marker}-desc">{esc(description)}</desc>
<defs>
  <marker id="{marker}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="{COLORS['line']}"/>
  </marker>
</defs>
{body}
</svg>'''


def example_streams() -> tuple[str, str]:
    marker = "example-arrow"
    parts = [text(24, 27, "Running example: stream dependencies", anchor="start", cls="section")]
    nodes = [(55, "x", "blue"), (245, "scaled", "green"), (435, "total", "orange"), (625, "alert", "purple")]
    for x, label, color in nodes:
        parts.append(box(x, 48, 120, 44, label, fill=f"{color}_fill", stroke=color))
    for x1, x2 in [(175, 245), (365, 435), (555, 625)]:
        parts.append(arrow(x1, 70, x2, 70, marker))
    parts.extend(
        [
            '<path d="M 495 48 C 495 10, 555 10, 555 48" fill="none" '
            f'stroke="{COLORS["orange"]}" stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
            text(525, 14, "previous tick", cls="small"),
        ]
    )
    title = "Stream dependencies in the running example"
    desc = "Input x flows through scaled, total, and alert on one tick. Total also reads its own previous-tick output."
    return svg(title, desc, 800, 115, "\n".join(parts), marker), "Solid arrows are same-tick dependencies; the dashed loop is retained previous-tick state."


def pipeline() -> tuple[str, str]:
    marker = "pipeline-arrow"
    parts = [text(24, 27, "Compile once", anchor="start", cls="section")]
    compile_nodes = [
        (25, 48, 135, "DSRV spec", "blue", "typed or untyped"),
        (205, 48, 145, "LoweredProgram", "green", "plans + free vars"),
        (395, 48, 165, "DepGraph order", "orange", "dependencies first"),
        (605, 48, 145, "into_monitor", "purple", "layout + bind"),
        (780, 48, 130, "DataflowMonitor", "blue", "executors"),
    ]
    for x, y, w, label, color, sublabel in compile_nodes:
        parts.append(box(x, y, w, 62, label, fill=f"{color}_fill", stroke=color, sublabel=sublabel))
    for x1, x2 in [(160, 205), (350, 395), (560, 605), (750, 780)]:
        parts.append(arrow(x1, 79, x2, 79, marker))
    parts.extend(
        [
            f'<line x1="25" y1="137" x2="895" y2="137" stroke="{COLORS["border"]}" stroke-width="1"/>',
            text(24, 165, "Evaluate every tick", anchor="start", cls="section"),
        ]
    )
    tick_nodes = [
        (55, 185, 135, "input row", "blue", "one logical tick"),
        (250, 185, 170, "environment row", "green", "inputs, then results"),
        (480, 185, 180, "PlanExecutor x N", "orange", "dependency order"),
        (720, 185, 150, "output row", "purple", "project slots"),
    ]
    for x, y, w, label, color, sublabel in tick_nodes:
        parts.append(box(x, y, w, 62, label, fill=f"{color}_fill", stroke=color, sublabel=sublabel))
    for x1, x2 in [(190, 250), (420, 480), (660, 720)]:
        parts.append(arrow(x1, 216, x2, 216, marker))
    parts.append(arrow(845, 110, 570, 185, marker, dashed=True))
    parts.append(text(725, 151, "owns and reuses", cls="small"))
    title = "Dataflow monitor compilation and evaluation pipeline"
    desc = "A specification becomes a LoweredProgram whose dependency graph orders plans before into_monitor lays out slots, binds references, and creates a DataflowMonitor. Every tick loads an input row, extends an environment through ordered executors, and projects an output row."
    return svg(title, desc, 920, 270, "\n".join(parts), marker), "Compilation creates the ordered monitor; evaluation reuses it for each logical input row."


def environment_layout() -> tuple[str, str]:
    marker = "environment-arrow"
    parts = [text(24, 27, "Inputs, environment slots, and output projection (tick 2)", anchor="start", cls="section")]
    parts.extend(
        [
            text(35, 58, "Value source", anchor="start", cls="label"),
            text(350, 58, "shared environment row", anchor="start", cls="label"),
            text(720, 58, "illustrative output row", anchor="start", cls="label"),
            box(35, 75, 225, 42, "input[0]: x = 8", fill="blue_fill", stroke="blue"),
            box(35, 130, 225, 42, "1. scaled -> 16", fill="green_fill", stroke="green"),
            box(35, 185, 225, 42, "2. total -> 24", fill="orange_fill", stroke="orange"),
            box(35, 240, 225, 42, "3. alert -> true", fill="purple_fill", stroke="purple"),
            box(350, 75, 230, 42, "slot 0: x = 8", fill="blue_fill", stroke="blue"),
            box(350, 130, 230, 42, "slot 1: scaled = 16", fill="green_fill", stroke="green"),
            box(350, 185, 230, 42, "slot 2: total = 24", fill="orange_fill", stroke="orange"),
            box(350, 240, 230, 42, "slot 3: alert = true", fill="purple_fill", stroke="purple"),
            box(720, 75, 175, 42, "output[0]: alert", fill="purple_fill", stroke="purple"),
            box(720, 157, 175, 42, "output[1]: total", fill="orange_fill", stroke="orange"),
            box(720, 240, 175, 42, "output[2]: scaled", fill="green_fill", stroke="green"),
            arrow(260, 96, 350, 96, marker),
            arrow(260, 151, 350, 151, marker),
            arrow(260, 206, 350, 206, marker),
            arrow(260, 261, 350, 261, marker),
            '<path d="M 580 261 C 640 261, 655 96, 720 96" fill="none" '
            f'stroke="{COLORS["purple"]}" stroke-width="2" marker-end="url(#{marker})"/>',
            '<path d="M 580 206 C 640 206, 655 178, 720 178" fill="none" '
            f'stroke="{COLORS["orange"]}" stroke-width="2" marker-end="url(#{marker})"/>',
            '<path d="M 580 151 C 640 151, 655 261, 720 261" fill="none" '
            f'stroke="{COLORS["green"]}" stroke-width="2" marker-end="url(#{marker})"/>',
            text(720, 310, "output EnvironmentIds = [3, 2, 1]", anchor="start", cls="code"),
            text(35, 310, "stable slots; executor order may change independently", anchor="start", cls="small"),
        ]
    )
    title = "Mapping inputs and computed streams through environment slots to outputs"
    desc = "At tick two, input x occupies environment slot zero. Scaled, total, and alert write values to stable slots one through three. For the illustrated monitor output order, alert, total, and scaled project slots three, two, and one. Runtime scheduling may change executor order without changing these slots."
    return svg(title, desc, 940, 335, "\n".join(parts), marker), "Environment slots remain stable even if runtime dependencies reorder executors; outputs project their own API order through saved IDs."


def history_retention() -> tuple[str, str]:
    marker = "history-arrow"
    parts = [text(24, 27, "Bounded history ownership", anchor="start", cls="section")]
    parts.extend(
        [
            text(35, 58, "x[3] before tick 4 push", anchor="start", cls="label"),
            box(35, 78, 82, 54, "10", fill="orange_fill", stroke="orange", sublabel="oldest"),
            box(127, 78, 82, 54, "20", fill="green_fill", stroke="green"),
            box(219, 78, 82, 54, "30", fill="green_fill", stroke="green"),
            box(35, 168, 110, 54, "x = 40", fill="blue_fill", stroke="blue", sublabel="push here"),
            box(191, 168, 110, 54, "result = 10", fill="purple_fill", stroke="purple", sublabel="read oldest"),
            arrow(90, 168, 76, 132, marker),
            arrow(76, 132, 246, 168, marker),
            f'<line x1="340" y1="48" x2="340" y2="232" stroke="{COLORS["border"]}" stroke-width="1"/>',
            text(375, 58, "Plan state", anchor="start", cls="label"),
            box(375, 78, 175, 54, "PlanExecutor", fill="blue_fill", stroke="blue", sublabel="owns DataflowState"),
            box(600, 60, 250, 48, "Delay x[3]", fill="orange_fill", stroke="orange", sublabel="ring capacity 3"),
            box(600, 118, 250, 48, "Recursive z[2]", fill="orange_fill", stroke="orange", sublabel="commit after output"),
            box(375, 178, 175, 54, "DynamicState", fill="purple_fill", stroke="purple", sublabel="active executor"),
            box(600, 180, 250, 48, "Delay x[2]", fill="green_fill", stroke="green", sublabel="installation lifetime"),
            arrow(550, 105, 600, 84, marker),
            arrow(550, 105, 600, 142, marker),
            arrow(550, 205, 600, 204, marker),
        ]
    )
    title = "Bounded dataflow history and ownership"
    desc = "A three-entry ring for x indexed by three reads the oldest value before replacing it. Static and recursive delays belong to a PlanExecutor, while a dynamic delay belongs to the currently installed executor."
    return svg(title, desc, 885, 255, "\n".join(parts), marker), "Each index owns a fixed-size ring; recursive history commits after output, and dynamic history belongs to the installed executor."


def plan_body() -> tuple[str, str]:
    marker = "plan-arrow"
    parts = [text(24, 27, "Bound PlanBody for total", anchor="start", cls="section")]
    plan_nodes = [
        (40, 60, 145, "RecursiveSIndex", "orange"),
        (230, 60, 125, "Default", "orange"),
        (230, 120, 125, "env: scaled", "green"),
        (410, 90, 105, "Add", "blue"),
        (570, 90, 115, "output", "purple"),
    ]
    for x, y, w, label, color in plan_nodes:
        parts.append(box(x, y, w, 38, label, fill=f"{color}_fill", stroke=color))
    parts.extend(
        [
            arrow(185, 79, 230, 79, marker),
            arrow(355, 79, 410, 102, marker),
            arrow(355, 139, 410, 116, marker),
            arrow(515, 109, 570, 109, marker),
            '<path d="M 627 128 C 627 198, 112 198, 112 98" fill="none" '
            f'stroke="{COLORS["orange"]}" stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
            text(365, 205, "commit completed output to recursive delay", cls="small"),
        ]
    )
    title = "Operation order in the total plan body"
    desc = "RecursiveSIndex and the scaled environment operand feed Default and Add. The completed output is committed back to the recursive delay after the forward pass."
    return svg(title, desc, 760, 220, "\n".join(parts), marker), "Solid arrows are forward-pass reads; the dashed path is the post-output recursive-delay commit."


def lazy_if() -> tuple[str, str]:
    marker = "if-arrow"
    parts = [text(24, 27, "Lazy branch selection and state ownership", anchor="start", cls="section")]
    parts.extend(
        [
            box(35, 112, 140, 48, "condition", fill="blue_fill", stroke="blue", sublabel="outer plan operand"),
            box(245, 105, 145, 62, "evaluate branches", fill="panel", stroke="border", sublabel="advance both states"),
            arrow(175, 136, 245, 136, marker),
            box(480, 48, 180, 70, "then PlanBody", fill="green_fill", stroke="green", sublabel="then DataflowState"),
            box(480, 190, 180, 70, "else PlanBody", fill="orange_fill", stroke="orange", sublabel="else DataflowState"),
            arrow(390, 125, 480, 83, marker),
            arrow(390, 147, 480, 225, marker),
            text(420, 92, "advance", cls="small"),
            text(420, 205, "advance", cls="small"),
            box(735, 105, 140, 62, "select result", fill="purple_fill", stroke="purple", sublabel="suppress other error"),
            arrow(660, 83, 735, 125, marker),
            arrow(660, 225, 735, 147, marker),
            '<path d="M 317 167 L 317 287 L 805 287 L 805 167" fill="none" '
            f'stroke="{COLORS["line"]}" stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
            text(545, 305, "NoVal / Deferred: advance both states, return condition value", cls="small"),
        ]
    )
    title = "Lazy if branch execution"
    desc = "Both nested PlanBody states and lifted outputs advance on every tick. Selection remains lazy with respect to errors."
    return svg(title, desc, 920, 325, "\n".join(parts), marker), "Each branch owns independent persistent state; selection suppresses errors from the unselected branch."


def function_binding() -> tuple[str, str]:
    marker = "function-binding-arrow"
    parts = [text(24, 27, "Function definition after binding", anchor="start", cls="section")]
    parts.extend(
        [
            box(35, 62, 170, 62, "outer environment", fill="blue_fill", stroke="blue", sublabel="bias, n, streams"),
            box(275, 55, 205, 76, "capture_sources", fill="green_fill", stroke="green", sublabel="resolved EnvironmentIds"),
            arrow(205, 93, 275, 93, marker),
            box(550, 48, 300, 90, "DataflowFunctionDef", fill="purple_fill", stroke="purple", sublabel="params + capture slots + body plan"),
            arrow(480, 93, 550, 93, marker),
            text(445, 78, "store", cls="small"),
            box(550, 158, 300, 62, "shared ExecutablePlan", fill="panel", stroke="border", sublabel="body layout: [captures | params]"),
            arrow(700, 138, 700, 158, marker),
        ]
    )
    title = "Function capture binding"
    desc = "Free variables are resolved to outer EnvironmentIds and stored with parameters and a shared executable body plan in DataflowFunctionDef."
    return svg(title, desc, 900, 245, "\n".join(parts), marker), "Binding stores capture source EnvironmentIds and gives the shared body plan a captures-first local layout."


def function_call() -> tuple[str, str]:
    marker = "function-call-arrow"
    parts = [text(24, 27, "Function evaluation and calls", anchor="start", cls="section")]
    parts.extend(
        [
            box(35, 62, 160, 62, "Function node", fill="blue_fill", stroke="blue", sublabel="current tick"),
            box(260, 55, 205, 76, "call template", fill="green_fill", stroke="green", sublabel="[capture values | params]"),
            arrow(195, 93, 260, 93, marker),
            text(228, 80, "snapshot", cls="small"),
            box(535, 48, 220, 90, "pooled call frame", fill="orange_fill", stroke="orange", sublabel="fill args; reset executor"),
            arrow(465, 93, 535, 93, marker),
            box(535, 170, 220, 62, "shared body plan", fill="panel", stroke="border", sublabel="evaluate one value"),
            arrow(645, 138, 645, 170, marker),
            text(790, 91, "return frame to pool", anchor="start", cls="small"),
            '<path d="M 755 201 C 850 201, 850 93, 755 93" fill="none" '
            f'stroke="{COLORS["line"]}" stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
            text(300, 214, "Apply / map / filter / fold create isolated invocations", cls="small"),
        ]
    )
    title = "Function call frame lifecycle"
    desc = "Evaluating a function node snapshots captures. Each invocation fills a pooled resettable frame, evaluates the shared body plan, and returns the frame to the pool."
    return svg(title, desc, 900, 255, "\n".join(parts), marker), "Capture values belong to the current tick; argument and node state belongs to one resettable invocation frame."


def dynamic_dependencies() -> tuple[str, str]:
    marker = "dynamic-dependencies-arrow"

    def panel(x: int, y: int, width: int, height: int, fill: str, stroke: str) -> str:
        return (
            f'<rect x="{x}" y="{y}" width="{width}" height="{height}" rx="5" '
            f'fill="{COLORS[fill]}" stroke="{COLORS[stroke]}" stroke-width="1.5"/>'
        )

    def dependency_row(
        y: int,
        tick: str,
        definitions: str,
        edge: str,
        sort_note: str,
        order: str,
        order_note: str,
        order_fill: str,
        order_stroke: str,
    ) -> list[str]:
        label_y = y + 24
        note_y = y + 43
        return [
            (
                f'<text x="35" y="{label_y}" font-family="system-ui, sans-serif" '
                f'font-size="13" font-weight="700" fill="{COLORS["ink"]}">{esc(tick)}</text>'
            ),
            panel(105, y, 250, 58, "blue_fill", "blue"),
            text(230, label_y, definitions, cls="code"),
            text(230, note_y, edge, cls="small"),
            panel(425, y, 190, 58, "orange_fill", "orange"),
            text(520, label_y, "topological sort", cls="code"),
            text(520, note_y, sort_note, cls="small"),
            panel(685, y, 190, 58, order_fill, order_stroke),
            text(780, label_y, order, cls="code"),
            text(780, note_y, order_note, cls="small"),
            arrow(355, y + 29, 425, y + 29, marker),
            arrow(615, y + 29, 685, y + 29, marker),
        ]

    parts = [text(24, 28, "Active definitions determine the order", anchor="start", cls="section")]
    parts.extend(
        dependency_row(
            44,
            "tick n",
            'a = "b + 1"; b = "x"',
            "actual computed edge: b → a",
            "static + active edges",
            "order [b, a]",
            "write fixed slots",
            "green_fill",
            "green",
        )
    )
    parts.extend(
        dependency_row(
            122,
            "tick n+1",
            'a = "x"; b = "a + 1"',
            "replace together: a → b",
            "reject cycle if present",
            "order [a, b]",
            "same EnvironmentIds",
            "purple_fill",
            "purple",
        )
    )
    parts.extend(
        [
            f'<line x1="25" y1="207" x2="895" y2="207" stroke="{COLORS["border"]}" stroke-width="1"/>',
            text(24, 237, "One reordering tick is a transaction", anchor="start", cls="section"),
            panel(25, 258, 150, 62, "panel", "border"),
            text(100, 285, "snapshot state", cls="code"),
            text(100, 304, "before the tick", cls="small"),
            panel(220, 258, 180, 62, "blue_fill", "blue"),
            text(310, 285, "evaluate + collect", cls="code"),
            text(310, 304, "all active free variables", cls="small"),
            panel(445, 258, 185, 62, "orange_fill", "orange"),
            text(537, 285, "replace edges + sort", cls="code"),
            text(537, 304, "changes are atomic", cls="small"),
            panel(675, 258, 220, 62, "green_fill", "green"),
            text(785, 285, "restore + retry if changed", cls="code"),
            text(785, 304, "commit one state advance", cls="small"),
            arrow(175, 289, 220, 289, marker),
            arrow(400, 289, 445, 289, marker),
            arrow(630, 289, 675, 289, marker),
            (
                '<path d="M 785 320 C 785 365, 310 365, 310 320" fill="none" '
                f'stroke="{COLORS["line"]}" stroke-width="1.8" stroke-dasharray="5 4" '
                f'marker-end="url(#{marker})"/>'
            ),
            text(
                548,
                388,
                "repeat until observed dependencies match the evaluation order",
                cls="small",
            ),
        ]
    )
    title = "Transactional reordering for changing dynamic dependencies"
    desc = "On one tick, definitions b plus one and x create edge b to a and order b then a. On a later tick, definitions x and a plus one atomically replace the active edges and produce order a then b. The monitor snapshots state, evaluates and collects edges, checks for cycles, sorts, and retries if the order changed while environment slots remain fixed."
    caption = "Changing both definitions replaces their active edges as one transaction and reverses the executor order without changing environment IDs."
    return svg(title, desc, 920, 405, "\n".join(parts), marker), caption


def runtime_compile_prefix(marker: str) -> list[str]:
    return [
        box(35, 45, 140, 50, "source string", fill="blue_fill", stroke="blue"),
        box(235, 39, 320, 62, "parse -> type/scope check -> bind", fill="panel", stroke="border", sublabel="reuse outer EnvironmentLayout"),
        box(625, 39, 210, 62, "PlanExecutor", fill="purple_fill", stroke="purple", sublabel="fresh persistent state"),
        arrow(175, 70, 235, 70, marker),
        arrow(555, 70, 625, 70, marker),
    ]


def dynamic_lifecycle() -> tuple[str, str]:
    marker = "dynamic-arrow"
    parts = [text(24, 27, "dynamic: replace when source changes", anchor="start", cls="section")]
    parts.extend(runtime_compile_prefix(marker))
    dynamic_cells = [
        (90, '"x + 1"', "compile; install executor", "green"),
        (285, '"x + 1"', "reuse executor + state", "green"),
        (480, '"x * 2"', "compile; reset + replace", "orange"),
        (675, "NoVal", "reuse x * 2; lift result", "purple"),
    ]
    for x, source, action, color in dynamic_cells:
        parts.append(box(x, 145, 165, 62, source, fill=f"{color}_fill", stroke=color, sublabel=action))
    for x1, x2 in [(255, 285), (450, 480), (645, 675)]:
        parts.append(arrow(x1, 176, x2, 176, marker))
    title = "Dynamic plan replacement lifecycle"
    desc = "Dynamic compiles the first source, reuses its executor for an equal string, resets executor and retained output for a changed string, and stream-lifts NoVal to reuse the active definition."
    return svg(title, desc, 900, 230, "\n".join(parts), marker), "Equal source strings preserve executor state; a changed string installs a fresh executor."


def defer_lifecycle() -> tuple[str, str]:
    marker = "defer-arrow"
    parts = [text(24, 27, "defer: pin the first accepted definition", anchor="start", cls="section")]
    parts.extend(runtime_compile_prefix(marker))
    defer_cells = [
        (90, "Deferred", "no active plan", "purple"),
        (285, '"x + 1"', "compile; install executor", "green"),
        (480, '"x * 2"', "ignore; tick x + 1", "green"),
        (675, "NoVal", "tick x + 1; emit result", "green"),
    ]
    for x, source, action, color in defer_cells:
        parts.append(box(x, 145, 165, 62, source, fill=f"{color}_fill", stroke=color, sublabel=action))
    for x1, x2 in [(255, 285), (450, 480), (645, 675)]:
        parts.append(arrow(x1, 176, x2, 176, marker))
    title = "Deferred definition lifecycle"
    desc = "Defer propagates Deferred before activation, installs the first source string, ignores later strings, and ticks the installed executor even when the source is NoVal."
    return svg(title, desc, 900, 230, "\n".join(parts), marker), "The first accepted string fixes the plan; every later tick advances that same executor."


DIAGRAMS = {
    "example-streams": example_streams,
    "pipeline": pipeline,
    "environment-layout": environment_layout,
    "history-retention": history_retention,
    "plan-body": plan_body,
    "lazy-if": lazy_if,
    "function-binding": function_binding,
    "function-call": function_call,
    "dynamic-dependencies": dynamic_dependencies,
    "dynamic-lifecycle": dynamic_lifecycle,
    "defer-lifecycle": defer_lifecycle,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if generated files differ")
    args = parser.parse_args()

    expected_assets: dict[Path, str] = {}
    for name, render in DIAGRAMS.items():
        svg_text, _caption = render()
        expected_assets[ASSET_DIR / f"{name}.svg"] = svg_text + "\n"

    stale: list[str] = []
    for path, expected in expected_assets.items():
        if not path.exists() or path.read_text(encoding="utf-8") != expected:
            stale.append(str(path.relative_to(ROOT)))
    obsolete_assets = sorted(
        path for path in ASSET_DIR.glob("*.svg") if path not in expected_assets
    )
    stale.extend(str(path.relative_to(ROOT)) for path in obsolete_assets)

    if args.check:
        if stale:
            print("stale generated dataflow diagrams:", file=sys.stderr)
            for path in stale:
                print(f"  {path}", file=sys.stderr)
            return 1
        return 0

    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    for path in obsolete_assets:
        path.unlink()
    for path, expected in expected_assets.items():
        path.write_text(expected, encoding="utf-8")
    print("updated dataflow diagrams:")
    for path in expected_assets:
        print(f"  {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
