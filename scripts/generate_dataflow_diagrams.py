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


def esc(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def text(
    x: int, y: int, value: str, *, anchor: str = "middle", cls: str = "label"
) -> str:
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


def arrow(
    x1: int, y1: int, x2: int, y2: int, marker: str, *, dashed: bool = False
) -> str:
    dash = ' stroke-dasharray="5 4"' if dashed else ""
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
        f'stroke="{COLORS["line"]}" stroke-width="1.8" marker-end="url(#{marker})"{dash}/>'
    )


def divider(y: int, width: int) -> str:
    return f'<line x1="25" y1="{y}" x2="{width - 25}" y2="{y}" stroke="{COLORS["border"]}" stroke-width="1"/>'


def svg(
    title: str, description: str, width: int, height: int, body: str, marker: str
) -> str:
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-labelledby="{marker}-title {marker}-desc" style="width:100%;max-width:{width}px;height:auto;background:#ffffff;border:1px solid #d0d7de;border-radius:6px">
<title id="{marker}-title">{esc(title)}</title>
<desc id="{marker}-desc">{esc(description)}</desc>
<defs>
  <marker id="{marker}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="{COLORS["line"]}"/>
  </marker>
</defs>
{body}
</svg>'''


def example_streams() -> tuple[str, str]:
    marker = "example-arrow"
    parts = [
        text(
            24,
            27,
            "Running example: stream dependencies",
            anchor="start",
            cls="section",
        )
    ]
    nodes = [
        (55, "x", "blue"),
        (245, "scaled", "green"),
        (435, "total", "orange"),
        (625, "alert", "purple"),
    ]
    for x, label, color in nodes:
        parts.append(box(x, 62, 120, 44, label, fill=f"{color}_fill", stroke=color))
    for x1, x2 in [(175, 245), (365, 435), (555, 625)]:
        parts.append(arrow(x1, 84, x2, 84, marker))
    parts.extend(
        [
            f'<path d="M 495 106 C 495 165, 555 165, 555 106" fill="none" stroke="{COLORS["orange"]}" stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
            text(525, 151, "previous tick: total[1]", cls="small"),
        ]
    )
    return (
        svg(
            "Stream dependencies in the running example",
            "Input x flows through scaled, total, and alert during one tick. Total also reads retained previous-tick state through its delayed self-reference.",
            800,
            180,
            "\n".join(parts),
            marker,
        ),
        "Solid arrows are same-tick dependencies; the dashed loop is retained previous-tick state.",
    )


def pipeline() -> tuple[str, str]:
    marker = "pipeline-arrow"
    parts = [text(24, 27, "Compile once", anchor="start", cls="section")]
    compile_nodes = [
        (35, 52, 190, "DSRV specification", "blue", "typed or untyped"),
        (280, 52, 190, "LoweredDataflow", "green", "EvaluationGraph + dependencies"),
        (525, 52, 190, "StreamProgram", "orange", "bind EnvironmentSlot refs"),
        (770, 52, 190, "DataflowMonitor", "purple", "ordered persistent machine"),
    ]
    for x, y, w, label, color, note in compile_nodes:
        parts.append(
            box(x, y, w, 64, label, fill=f"{color}_fill", stroke=color, sublabel=note)
        )
    for x1, x2 in [(225, 280), (470, 525), (715, 770)]:
        parts.append(arrow(x1, 84, x2, 84, marker))
    parts.extend(
        [
            divider(145, 1000),
            text(24, 176, "Every logical tick", anchor="start", cls="section"),
        ]
    )
    phases = [
        (35, "1  load inputs", "environment row"),
        (275, "2  evaluate sources", "MonitorPlan source order"),
        (515, "3  resolve programs", "exact active dependencies"),
        (755, "4  validate schedule", "Scheduler repairs if needed"),
        (155, "5  evaluate once", "active dependency order"),
        (395, "6  commit temporal", "post-row staged writes"),
        (635, "7  project outputs", "saved EnvironmentSlots"),
    ]
    for index, (x, label, note) in enumerate(phases):
        y = 202 if index < 4 else 308
        parts.append(
            box(
                x,
                y,
                205,
                64,
                label,
                fill="blue_fill" if index < 4 else "green_fill",
                stroke="blue" if index < 4 else "green",
                sublabel=note,
            )
        )
    for x1, x2 in [(240, 275), (480, 515), (720, 755)]:
        parts.append(arrow(x1, 234, x2, 234, marker))
    parts.extend(
        [
            f'<path d="M 857 266 C 857 290, 257 290, 257 308" fill="none" stroke="{COLORS["line"]}" stroke-width="1.8" marker-end="url(#{marker})"/>',
            arrow(360, 340, 395, 340, marker),
            arrow(600, 340, 635, 340, marker),
        ]
    )
    return (
        svg(
            "Current dataflow compilation and tick pipeline",
            "Compilation lowers EvaluationGraphs, binds EnvironmentSlots into StreamPrograms, and creates a DataflowMonitor with a MonitorPlan, Scheduler, and persistent stream evaluators. Each tick loads inputs, pre-evaluates expression sources, resolves active programs, validates the schedule, evaluates every remaining stream once in active dependency order, commits temporal state, and projects outputs.",
            1000,
            400,
            "\n".join(parts),
            marker,
        ),
        "Compilation creates the ordered monitor; evaluation reuses it for each logical input row.",
    )


def environment_layout() -> tuple[str, str]:
    marker = "environment-arrow"
    parts = [
        text(
            24,
            27,
            "Stable EnvironmentSlot layout (tick 2)",
            anchor="start",
            cls="section",
        )
    ]
    parts.extend(
        [
            text(35, 62, "producers", anchor="start", cls="label"),
            text(360, 62, "environment_values", anchor="start", cls="label"),
            text(735, 62, "output projection", anchor="start", cls="label"),
        ]
    )
    rows = [
        (82, "input[0]: x = 8", "EnvironmentSlot(0): x = 8", "blue"),
        (142, "scaled -> 16", "EnvironmentSlot(1): scaled = 16", "green"),
        (202, "total -> 24", "EnvironmentSlot(2): total = 24", "orange"),
        (262, "alert -> true", "EnvironmentSlot(3): alert = true", "purple"),
    ]
    for y, source, slot, color in rows:
        parts.extend(
            [
                box(35, y, 235, 44, source, fill=f"{color}_fill", stroke=color),
                box(360, y, 275, 44, slot, fill=f"{color}_fill", stroke=color),
                arrow(270, y + 22, 360, y + 22, marker),
            ]
        )
    outputs = [
        (82, "output[0]: alert", "purple", 284),
        (172, "output[1]: total", "orange", 224),
        (262, "output[2]: scaled", "green", 164),
    ]
    for y, label, color, source_y in outputs:
        parts.append(box(735, y, 190, 44, label, fill=f"{color}_fill", stroke=color))
        parts.append(
            f'<path d="M 635 {source_y} C 685 {source_y}, 685 {y + 22}, 735 {y + 22}" fill="none" stroke="{COLORS[color]}" stroke-width="1.8" marker-end="url(#{marker})"/>'
        )
    parts.extend(
        [
            text(
                35,
                346,
                "The scheduler may reorder stream evaluation; state and slots do not move",
                anchor="start",
                cls="small",
            ),
            text(735, 346, "output slots = [3, 2, 1]", anchor="start", cls="code"),
        ]
    )
    return (
        svg(
            "Stable environment slots and output projection",
            "Input and computed values occupy stable EnvironmentSlot indices. The scheduler may change stream evaluation order without moving evaluator state, while output slots continue to project alert, total, and scaled from slots three, two, and one.",
            960,
            375,
            "\n".join(parts),
            marker,
        ),
        "Environment slots remain stable even if runtime dependencies reorder evaluators; outputs project their own API order through saved EnvironmentSlots.",
    )


def history_retention() -> tuple[str, str]:
    marker = "history-arrow"
    parts = [
        text(
            24,
            27,
            "Read old state; commit new samples",
            anchor="start",
            cls="section",
        ),
        f'<rect x="25" y="48" width="440" height="350" rx="7" fill="{COLORS["panel"]}" stroke="{COLORS["border"]}"/>',
        f'<rect x="495" y="48" width="440" height="350" rx="7" fill="{COLORS["panel"]}" stroke="{COLORS["border"]}"/>',
        text(45, 76, "ordinary delay", anchor="start", cls="label"),
        text(515, 76, "recursive self-delay", anchor="start", cls="label"),
        box(
            55,
            95,
            170,
            58,
            "ring through n - 1",
            fill="blue_fill",
            stroke="blue",
            sublabel="committed history",
        ),
        box(
            265,
            95,
            170,
            58,
            "Delay",
            fill="orange_fill",
            stroke="orange",
            sublabel="read; mark pending",
        ),
        arrow(225, 124, 265, 124, marker),
        box(
            55,
            270,
            170,
            58,
            "row operand n",
            fill="green_fill",
            stroke="green",
            sublabel="completed row",
        ),
        box(
            265,
            270,
            170,
            58,
            "ordinary capture",
            fill="orange_fill",
            stroke="orange",
            sublabel="sample for this delay",
        ),
        arrow(225, 299, 265, 299, marker),
        f'<line x1="350" y1="153" x2="350" y2="270" stroke="{COLORS["line"]}" stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
        text(362, 218, "pending", anchor="start", cls="small"),
        box(
            525,
            95,
            170,
            58,
            "ring through n - 1",
            fill="blue_fill",
            stroke="blue",
            sublabel="committed history",
        ),
        box(
            735,
            95,
            170,
            58,
            "RecursiveDelay",
            fill="purple_fill",
            stroke="purple",
            sublabel="previous output",
        ),
        arrow(695, 124, 735, 124, marker),
        box(
            525,
            190,
            170,
            58,
            "current operands n",
            fill="green_fill",
            stroke="green",
        ),
        box(
            735,
            190,
            170,
            58,
            "stream body",
            fill="green_fill",
            stroke="green",
            sublabel="combine current + previous",
        ),
        arrow(695, 219, 735, 219, marker),
        arrow(820, 153, 820, 190, marker),
        box(
            735,
            285,
            170,
            58,
            "stream output n",
            fill="purple_fill",
            stroke="purple",
            sublabel="stage for self-delay",
        ),
        arrow(820, 248, 820, 285, marker),
        divider(420, 960),
        text(35, 448, "after row n completes", anchor="start", cls="label"),
        box(
            350,
            445,
            210,
            60,
            "temporal commit",
            fill="green_fill",
            stroke="green",
            sublabel="push staged samples",
        ),
        arrow(350, 328, 415, 445, marker),
        arrow(820, 343, 505, 445, marker),
        box(
            650,
            445,
            255,
            60,
            "rings through n",
            fill="blue_fill",
            stroke="blue",
            sublabel="readable at tick n + 1",
        ),
        arrow(560, 475, 650, 475, marker),
    ]
    return (
        svg(
            "Temporal delay staging and commit",
            "During tick n, an ordinary Delay and a RecursiveDelay each read only their own ring committed through tick n minus one. The recursive delay supplies a previous stream output to the stream body; after the body combines it with current operands, the completed stream output is staged for that same self-delay. The ordinary delay captures its operand from the completed row. The temporal commit pushes both staged samples into their respective rings, where they become readable at tick n plus one.",
            960,
            525,
            "\n".join(parts),
            marker,
        ),
        "Both delay forms read history committed before tick n; their new samples enter their respective rings together at the end-of-tick temporal commit.",
    )


def evaluation_graph() -> tuple[str, str]:
    marker = "graph-arrow"
    parts = [
        text(24, 27, "Bound EvaluationGraph for total", anchor="start", cls="section")
    ]
    nodes = [
        (45, 65, 210, "RecursiveDelay { 1 }", "orange", "retained previous total"),
        (45, 145, 210, "Const(0)", "green", "fallback before history exists"),
        (325, 105, 170, "Default", "orange", "node 0 or Const(0)"),
        (325, 205, 170, "External(slot 1)", "green", "scaled from EnvironmentSlot"),
        (565, 155, 135, "Add", "blue", "node 2"),
        (770, 155, 135, "output", "purple", "DataRef::Node"),
    ]
    for x, y, w, label, color, note in nodes:
        parts.append(
            box(x, y, w, 62, label, fill=f"{color}_fill", stroke=color, sublabel=note)
        )
    parts.extend(
        [
            arrow(255, 96, 325, 126, marker),
            arrow(255, 176, 325, 146, marker),
            arrow(495, 136, 565, 176, marker),
            arrow(495, 236, 565, 196, marker),
            arrow(700, 186, 770, 186, marker),
            f'<path d="M 837 155 L 837 48 L 150 48 L 150 65" fill="none" stroke="{COLORS["orange"]}" stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
            text(
                500,
                322,
                "post-output: stage RecursiveDelay, then commit after the completed row",
                cls="small",
            ),
        ]
    )
    return (
        svg(
            "Bound evaluation graph with recursive fallback",
            "The total EvaluationGraph reads a RecursiveDelay and uses Const zero as Default's fallback, adds scaled from an EnvironmentSlot, and returns the Add node. The completed output is staged for the recursive delay and committed after the row.",
            960,
            345,
            "\n".join(parts),
            marker,
        ),
        "Solid arrows are forward-pass reads; the dashed path is the post-output recursive-delay commit.",
    )


def specialization_overlay() -> tuple[str, str]:
    marker = "specialization-arrow"
    parts = [
        text(
            24,
            27,
            "One semantic graph with a mixed specialization overlay",
            anchor="start",
            cls="section",
        ),
        text(35, 61, "canonical authority", anchor="start", cls="label"),
        box(
            35,
            78,
            265,
            66,
            "BoundEvaluationGraph",
            fill="blue_fill",
            stroke="blue",
            sublabel="all semantic operations remain",
        ),
        box(
            340,
            78,
            265,
            66,
            "StreamState",
            fill="blue_fill",
            stroke="blue",
            sublabel="node_values + canonical NodeState",
        ),
        box(
            645,
            78,
            350,
            66,
            "environment_values",
            fill="blue_fill",
            stroke="blue",
            sublabel="every result is published as Value",
        ),
        arrow(300, 111, 340, 111, marker),
        arrow(605, 111, 645, 111, marker),
        divider(174, 1030),
        text(35, 205, "optional parallel plan and state", anchor="start", cls="label"),
        box(
            35,
            225,
            215,
            70,
            "Scalar Add",
            fill="green_fill",
            stroke="green",
            sublabel="typed sources + lift state",
        ),
        box(
            295,
            225,
            215,
            70,
            "Canonical Default",
            fill="orange_fill",
            stroke="orange",
            sublabel="normal interpreter node",
        ),
        box(
            555,
            225,
            215,
            70,
            "Scalar >",
            fill="green_fill",
            stroke="green",
            sublabel="may read canonical scalar",
        ),
        box(
            815,
            225,
            180,
            70,
            "ScalarValue",
            fill="purple_fill",
            stroke="purple",
            sublabel="mirror to canonical Value",
        ),
        arrow(250, 260, 295, 260, marker),
        arrow(510, 260, 555, 260, marker),
        arrow(770, 260, 815, 260, marker),
        f'<path d="M 925 225 C 925 185, 825 185, 825 144" fill="none" '
        f'stroke="{COLORS["purple"]}" stroke-width="1.8" marker-end="url(#{marker})"/>',
        box(
            35,
            340,
            270,
            66,
            "runtime kind mismatch",
            fill="orange_fill",
            stroke="orange",
            sublabel="transfer retained lifted operands",
        ),
        box(
            380,
            340,
            270,
            66,
            "Deoptimized",
            fill="orange_fill",
            stroke="orange",
            sublabel="this node stays canonical",
        ),
        box(
            725,
            340,
            270,
            66,
            "neighbouring scalar nodes",
            fill="green_fill",
            stroke="green",
            sublabel="continue unchanged",
        ),
        arrow(305, 373, 380, 373, marker),
        arrow(650, 373, 725, 373, marker, dashed=True),
        f'<path d="M 380 373 C 275 373, 275 174, 340 130" fill="none" '
        f'stroke="{COLORS["orange"]}" stroke-width="1.8" marker-end="url(#{marker})"/>',
    ]
    return (
        svg(
            "Mixed scalar specialization beside the canonical graph",
            "The canonical BoundEvaluationGraph, StreamState, and environment Value row remain authoritative. An optional plan places scalar and canonical instructions side by side. Scalar results mirror to canonical Values. A runtime kind mismatch transfers retained lifting state and permanently deoptimizes only that node, while neighbouring scalar nodes continue.",
            1030,
            435,
            "\n".join(parts),
            marker,
        ),
        "The scalar plan is a specialization overlay on the canonical graph; deoptimization is persistent but local to one planned node.",
    )


def execution_layout() -> tuple[str, str]:
    marker = "layout-arrow"
    parts = [
        text(
            24,
            27,
            "Logical plan, replaceable routing, fixed state",
            anchor="start",
            cls="section",
        ),
        box(
            35,
            52,
            270,
            72,
            "MonitorPlan",
            fill="blue_fill",
            stroke="blue",
            sublabel="slots, dependencies, commit set",
        ),
        box(
            380,
            52,
            270,
            72,
            "Scheduler",
            fill="orange_fill",
            stroke="orange",
            sublabel="current dependency-first order",
        ),
        box(
            725,
            52,
            270,
            72,
            "ExecutionLayout",
            fill="purple_fill",
            stroke="purple",
            sublabel="routing steps only; ≤ 4 cached",
        ),
        arrow(305, 88, 380, 88, marker),
        arrow(650, 88, 725, 88, marker),
        text(35, 166, "selected layout steps", anchor="start", cls="label"),
        box(
            35,
            184,
            280,
            66,
            "ScalarRun [stream 0, 1]",
            fill="green_fill",
            stroke="green",
            sublabel="two complete one-node streams",
        ),
        box(
            375,
            184,
            280,
            66,
            "Graph stream 2",
            fill="orange_fill",
            stroke="orange",
            sublabel="may use a mixed scalar plan",
        ),
        box(
            715,
            184,
            280,
            66,
            "ScalarRun [stream 3]",
            fill="green_fill",
            stroke="green",
            sublabel="consumes stream 2 if scalar",
        ),
        arrow(315, 217, 375, 217, marker),
        arrow(655, 217, 715, 217, marker),
        text(35, 292, "fixed EvaluatorArena", anchor="start", cls="label"),
        box(
            35,
            310,
            215,
            72,
            "evaluator 0",
            fill="panel",
            stroke="border",
            sublabel="canonical + scalar state",
        ),
        box(
            285,
            310,
            215,
            72,
            "evaluator 1",
            fill="panel",
            stroke="border",
            sublabel="canonical + scalar state",
        ),
        box(
            535,
            310,
            215,
            72,
            "evaluator 2",
            fill="panel",
            stroke="border",
            sublabel="functions, delays, dynamic",
        ),
        box(
            785,
            310,
            210,
            72,
            "evaluator 3",
            fill="panel",
            stroke="border",
            sublabel="canonical + scalar state",
        ),
        f'<path d="M 145 250 L 145 310" fill="none" stroke="{COLORS["line"]}" '
        f'stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
        f'<path d="M 245 250 L 392 310" fill="none" stroke="{COLORS["line"]}" '
        f'stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
        f'<path d="M 515 250 L 642 310" fill="none" stroke="{COLORS["line"]}" '
        f'stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
        f'<path d="M 855 250 L 890 310" fill="none" stroke="{COLORS["line"]}" '
        f'stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#{marker})"/>',
        text(
            515,
            418,
            "layout replacement changes routing, never evaluator identity or language state",
            cls="small",
        ),
    ]
    return (
        svg(
            "Execution layouts route into a fixed evaluator arena",
            "The immutable MonitorPlan feeds a Scheduler whose dependency-first order selects a replaceable and cacheable ExecutionLayout. ScalarRun and Graph steps address stable evaluator IDs in the fixed arena. Graph steps may still use mixed specialization. Layout replacement never moves histories, function frames, dynamic evaluators, or deoptimization state.",
            1030,
            445,
            "\n".join(parts),
            marker,
        ),
        "Schedules choose compact routing steps over stable arena IDs; all semantic and specialization state stays in the arena.",
    )


def lazy_if() -> tuple[str, str]:
    marker = "if-arrow"
    parts = [
        text(
            24,
            27,
            "Lazy if: ordinary timelines and recursive base cases",
            anchor="start",
            cls="section",
        )
    ]
    parts.extend(
        [
            box(
                35,
                78,
                190,
                64,
                "condition",
                fill="blue_fill",
                stroke="blue",
                sublabel="retained stream-lifted value",
            ),
            box(
                310,
                52,
                235,
                64,
                "then EvaluationGraph",
                fill="green_fill",
                stroke="green",
                sublabel="own persistent StreamState",
            ),
            box(
                310,
                142,
                235,
                64,
                "else EvaluationGraph",
                fill="orange_fill",
                stroke="orange",
                sublabel="own persistent StreamState",
            ),
            arrow(225, 100, 310, 84, marker),
            arrow(225, 120, 310, 174, marker),
            box(
                630,
                92,
                260,
                74,
                "ordinary if",
                fill="purple_fill",
                stroke="purple",
                sublabel="advance both; select one result",
            ),
            arrow(545, 84, 630, 116, marker),
            arrow(545, 174, 630, 142, marker),
            divider(235, 930),
            box(
                35,
                265,
                240,
                68,
                "recursive call context",
                fill="blue_fill",
                stroke="blue",
                sublabel="genuinely lazy selection",
            ),
            box(
                345,
                265,
                240,
                68,
                "Bool condition",
                fill="green_fill",
                stroke="green",
                sublabel="evaluate selected branch only",
            ),
            box(
                655,
                265,
                240,
                68,
                "Deferred or NoVal",
                fill="orange_fill",
                stroke="orange",
                sublabel="evaluate neither branch",
            ),
            arrow(275, 299, 345, 299, marker),
            f'<path d="M 275 315 C 430 390, 560 390, 655 315" fill="none" stroke="{COLORS["line"]}" stroke-width="1.8" marker-end="url(#{marker})"/>',
            text(
                465,
                386,
                "Reconfiguration points inside either lazy branch are rejected during compilation",
                cls="small",
            ),
        ]
    )
    return (
        svg(
            "Conditional evaluation in ordinary and recursive contexts",
            "Ordinary conditionals evaluate both branch EvaluationGraphs so both persistent StreamStates advance before one result is selected. In recursive call context a Boolean evaluates only its selected branch, while Deferred or NoVal evaluates neither branch. Reconfiguration points inside lazy branches are rejected during compilation.",
            930,
            400,
            "\n".join(parts),
            marker,
        ),
        "Each branch owns independent persistent state and ordinarily advances every tick; recursive evaluation follows only a Boolean-selected branch and evaluates neither branch for Deferred or NoVal.",
    )


def function_binding() -> tuple[str, str]:
    marker = "function-binding-arrow"
    parts = [
        text(
            24,
            27,
            "Bind UnboundFunction into StreamFunction",
            anchor="start",
            cls="section",
        )
    ]
    parts.extend(
        [
            box(
                35,
                58,
                230,
                70,
                "UnboundFunction",
                fill="blue_fill",
                stroke="blue",
                sublabel="parameters + EvaluationGraph",
            ),
            box(
                335,
                58,
                250,
                70,
                "free vars - parameters",
                fill="green_fill",
                stroke="green",
                sublabel="resolve capture_slots",
            ),
            box(
                655,
                58,
                250,
                70,
                "StreamFunction",
                fill="purple_fill",
                stroke="purple",
                sublabel="parameters + display + program",
            ),
            arrow(265, 93, 335, 93, marker),
            arrow(585, 93, 655, 93, marker),
            box(
                175,
                190,
                250,
                70,
                "capture_slots",
                fill="orange_fill",
                stroke="orange",
                sublabel="Vec<EnvironmentSlot> in outer row",
            ),
            box(
                535,
                190,
                250,
                70,
                "Rc<StreamProgram>",
                fill="blue_fill",
                stroke="blue",
                sublabel="bound EvaluationGraph",
            ),
            arrow(460, 128, 300, 190, marker),
            arrow(780, 128, 660, 190, marker),
            box(
                355,
                310,
                250,
                64,
                "local EnvironmentLayout",
                fill="panel",
                stroke="border",
                sublabel="[captures | parameters]",
            ),
            arrow(300, 260, 430, 310, marker),
            arrow(660, 260, 530, 310, marker),
        ]
    )
    return (
        svg(
            "Current StreamFunction binding structures",
            "Binding removes parameters from an UnboundFunction's free variables, resolves captures to outer EnvironmentSlots, binds the EvaluationGraph against a captures-first local EnvironmentLayout, and stores the resulting StreamProgram in StreamFunction.",
            940,
            405,
            "\n".join(parts),
            marker,
        ),
        "Binding stores capture source environment slots and gives the shared body program a captures-first local layout.",
    )


def function_call() -> tuple[str, str]:
    marker = "function-call-arrow"
    parts = [
        text(
            24,
            27,
            "Normal and recursive function evaluation",
            anchor="start",
            cls="section",
        ),
        # Normal Apply: function identity selects one callable instance whose
        # evaluator state advances across logical ticks.
        f'<rect x="25" y="48" width="910" height="190" rx="8" fill="{COLORS["panel"]}" stroke="{COLORS["border"]}"/>',
        text(
            45,
            77,
            "Normal Apply: call-site state continues",
            anchor="start",
            cls="label",
        ),
        text(205, 105, "logical tick n", cls="small"),
        text(700, 105, "logical tick n + 1", cls="small"),
        box(45, 125, 150, 52, "F1(args)", fill="blue_fill", stroke="blue"),
        box(
            235,
            112,
            210,
            78,
            "callable F1",
            fill="blue_fill",
            stroke="blue",
            sublabel="state S(n)",
        ),
        arrow(195, 151, 235, 151, marker),
        box(555, 125, 150, 52, "F1(args')", fill="blue_fill", stroke="blue"),
        box(
            745,
            112,
            165,
            78,
            "same callable F1",
            fill="green_fill",
            stroke="green",
            sublabel="state S(n + 1)",
        ),
        arrow(705, 151, 745, 151, marker),
        f'<path d="M 445 175 C 520 220, 670 220, 745 175" fill="none" '
        f'stroke="{COLORS["green"]}" stroke-width="2" marker-end="url(#{marker})"/>',
        text(595, 218, "callable state", cls="small"),
        # RecursiveApply: every active depth gets a distinct reset evaluator
        # frame. Frames return to the per-call pool during unwind.
        f'<rect x="25" y="258" width="910" height="300" rx="8" fill="{COLORS["panel"]}" stroke="{COLORS["border"]}"/>',
        text(
            45,
            287,
            "RecursiveApply: reset frame at each call depth",
            anchor="start",
            cls="label",
        ),
        box(
            45,
            330,
            205,
            76,
            "RecursiveCall",
            fill="orange_fill",
            stroke="orange",
            sublabel="captures + shared program",
        ),
        arrow(250, 368, 365, 374, marker),
        f'<rect x="325" y="310" width="300" height="225" rx="7" fill="{COLORS["orange_fill"]}" stroke="{COLORS["orange"]}" stroke-width="1.5"/>',
        text(345, 335, "active call stack", anchor="start", cls="small"),
        box(
            365,
            350,
            220,
            48,
            "frame 0",
            fill="panel",
            stroke="border",
            sublabel="args: n",
        ),
        box(
            365,
            415,
            220,
            48,
            "frame 1",
            fill="panel",
            stroke="border",
            sublabel="args: n - 1",
        ),
        box(
            365,
            480,
            220,
            42,
            "frame 2",
            fill="panel",
            stroke="border",
            sublabel="base case",
        ),
        arrow(475, 398, 475, 415, marker),
        arrow(475, 463, 475, 480, marker),
        box(
            700,
            365,
            190,
            76,
            "available frame pool",
            fill="green_fill",
            stroke="green",
            sublabel="after unwind",
        ),
        f'<path d="M 625 500 C 680 500, 680 420, 700 403" fill="none" '
        f'stroke="{COLORS["line"]}" stroke-width="1.8" stroke-dasharray="5 4" '
        f'marker-end="url(#{marker})"/>',
    ]
    return (
        svg(
            "Normal Apply state continuation and recursive frame evaluation",
            "Normal Apply retains one callable instance while function identity F1 remains active. Its evaluator state advances from S n on one logical tick to S n plus one on the next; a different function identity creates fresh callable state. RecursiveApply creates a RecursiveCall with current captures and a shared program. Each active recursion depth acquires a separate frame, fills different arguments, resets its evaluator, and evaluates. Frames return to the available pool as recursion unwinds.",
            960,
            580,
            "\n".join(parts),
            marker,
        ),
        "Normal Apply carries one callable's state across ticks; RecursiveApply isolates active recursion depths in reset frames returned to a per-call pool.",
    )


def reconfiguration_points() -> tuple[str, str]:
    marker = "reconfiguration-points-arrow"

    def point_box(
        x: int,
        y: int,
        width: int,
        title: str,
        current: str,
        scope: str,
    ) -> str:
        center = x + width // 2
        return "\n".join(
            [
                f'<rect x="{x}" y="{y}" width="{width}" height="96" rx="7" '
                f'fill="{COLORS["orange_fill"]}" stroke="{COLORS["orange"]}" stroke-width="2"/>',
                text(center, y + 25, title, cls="code"),
                text(center, y + 51, f'current: "{current}"', cls="code"),
                text(center, y + 75, f"scope: {scope}", cls="small"),
            ]
        )

    parts = [
        text(
            24,
            27,
            "Current value flow for the example model",
            anchor="start",
            cls="section",
        ),
        # Legend: the graph intentionally shows only one inactive potential
        # edge, avoiding the dense all-potential graph used by the scheduler view.
        f'<rect x="35" y="48" width="22" height="18" rx="3" fill="{COLORS["orange_fill"]}" stroke="{COLORS["orange"]}" stroke-width="2"/>',
        text(67, 62, "reconfiguration point", anchor="start", cls="small"),
        arrow(255, 57, 315, 57, marker),
        text(327, 62, "active value flow", anchor="start", cls="small"),
        arrow(495, 57, 555, 57, marker, dashed=True),
        text(
            567, 62, "allowed value source, inactive now", anchor="start", cls="small"
        ),
        # Inputs and the fixed score calculation.
        box(55, 95, 120, 42, "sensor", fill="blue_fill", stroke="blue"),
        box(245, 95, 120, 42, "baseline", fill="blue_fill", stroke="blue"),
        box(735, 95, 120, 42, "enabled", fill="blue_fill", stroke="blue"),
        box(
            215,
            180,
            180,
            56,
            "score",
            fill="green_fill",
            stroke="green",
            sublabel="sensor - baseline",
        ),
        arrow(115, 137, 240, 180, marker),
        arrow(305, 137, 305, 180, marker),
        # Point 1 is also the complete right-hand side of stream limit.
        point_box(
            215,
            285,
            290,
            "Point 1: limit = defer(limit_source)",
            "score + 10",
            "score, baseline",
        ),
        arrow(305, 236, 305, 285, marker),
        f'<path d="M 365 116 C 565 145, 585 320, 505 330" fill="none" '
        f'stroke="{COLORS["line"]}" stroke-width="1.8" stroke-dasharray="5 4" '
        f'marker-end="url(#{marker})"/>',
        text(575, 235, "baseline is allowed but unused", cls="small"),
        # Point 2 and point 3 are subexpressions of one fixed decision equation.
        f'<rect x="85" y="415" width="770" height="185" rx="8" fill="{COLORS["panel"]}" stroke="{COLORS["border"]}" stroke-width="1.5"/>',
        text(105, 441, "decision = point 2 && point 3", anchor="start", cls="label"),
        point_box(
            115,
            465,
            285,
            "Point 2: dynamic(rule_source)",
            "score > limit",
            "score, limit",
        ),
        point_box(
            555,
            465,
            270,
            "Point 3: dynamic(gate_source)",
            "enabled",
            "enabled",
        ),
        # Active dependencies into point 2 are routed around point 1 so they do
        # not cross a node. Point 3 receives the preloaded enabled input.
        f'<path d="M 235 236 C 75 285, 65 430, 115 500" fill="none" '
        f'stroke="{COLORS["line"]}" stroke-width="1.8" marker-end="url(#{marker})"/>',
        arrow(330, 381, 300, 465, marker),
        f'<path d="M 795 137 C 895 245, 895 430, 825 500" fill="none" '
        f'stroke="{COLORS["line"]}" stroke-width="1.8" marker-end="url(#{marker})"/>',
        box(
            420,
            535,
            95,
            48,
            "decision",
            fill="green_fill",
            stroke="green",
        ),
        arrow(400, 535, 420, 553, marker),
        arrow(555, 535, 515, 553, marker),
    ]
    return (
        svg(
            "Current value flow through three reconfiguration points",
            "Sensor and baseline feed static stream score. Point one is the defer expression for limit; its current formula score plus ten activates score to limit, while baseline is permitted by the scope but is shown as one dashed inactive edge. The fixed decision equation contains point two and point three. Point two currently reads score and limit, so both feed it. Point three reads enabled. Their Boolean values are combined into the final decision output. Each orange point also names its source input and current formula.",
            920,
            620,
            "\n".join(parts),
            marker,
        ),
        "Orange boxes are the three reconfiguration points; solid arrows show current producer-to-consumer value flow, while the one dashed arrow is an allowed value source that the current formula does not use.",
    )


def dynamic_dependencies() -> tuple[str, str]:
    marker = "dynamic-dependencies-arrow"

    def permission_cell(x: int, y: int) -> str:
        return "\n".join(
            [
                f'<rect x="{x}" y="{y}" width="130" height="38" rx="5" '
                f'fill="{COLORS["blue_fill"]}" stroke="{COLORS["blue"]}" stroke-width="1.5"/>',
                text(x + 65, y + 27, "✓", cls="section"),
            ]
        )

    parts = [
        text(
            24,
            27,
            "Scope permissions, active dependencies, and schedule repair",
            anchor="start",
            cls="section",
        ),
        text(
            960,
            27,
            "dependency arrow: A -> B means A reads B",
            anchor="end",
            cls="small",
        ),
        # Compile-time permissions shown both as the full potential graph and
        # as a compact matrix.
        f'<rect x="25" y="48" width="950" height="180" rx="7" fill="{COLORS["panel"]}" stroke="{COLORS["border"]}"/>',
        text(45, 76, "Compile-time scope permissions", anchor="start", cls="label"),
        box(130, 94, 100, 38, "x", fill="blue_fill", stroke="blue"),
        box(60, 170, 100, 38, "a", fill="green_fill", stroke="green"),
        box(200, 170, 100, 38, "b", fill="purple_fill", stroke="purple"),
        arrow(110, 170, 155, 132, marker, dashed=True),
        arrow(250, 170, 205, 132, marker, dashed=True),
        f'<path d="M 160 181 C 177 151, 183 151, 200 181" fill="none" '
        f'stroke="{COLORS["line"]}" stroke-width="1.8" stroke-dasharray="5 4" '
        f'marker-end="url(#{marker})"/>',
        f'<path d="M 200 199 C 183 225, 177 225, 160 199" fill="none" '
        f'stroke="{COLORS["line"]}" stroke-width="1.8" stroke-dasharray="5 4" '
        f'marker-end="url(#{marker})"/>',
        box(390, 94, 130, 38, "stream", fill="panel", stroke="border"),
        box(520, 94, 130, 38, "x", fill="panel", stroke="border"),
        box(650, 94, 130, 38, "a", fill="panel", stroke="border"),
        box(780, 94, 130, 38, "b", fill="panel", stroke="border"),
        box(390, 132, 130, 38, "a", fill="green_fill", stroke="green"),
        permission_cell(520, 132),
        box(650, 132, 130, 38, "—", fill="panel", stroke="border"),
        permission_cell(780, 132),
        box(390, 170, 130, 38, "b", fill="purple_fill", stroke="purple"),
        permission_cell(520, 170),
        permission_cell(650, 170),
        box(780, 170, 130, 38, "—", fill="panel", stroke="border"),
        # Consecutive runtime ticks use stable columns and minimal annotation.
        f'<rect x="25" y="248" width="950" height="155" rx="7" fill="{COLORS["panel"]}" stroke="{COLORS["border"]}"/>',
        text(45, 276, "Runtime tick n", anchor="start", cls="label"),
        text(165, 300, "source values", cls="small"),
        text(535, 300, "active dependencies", cls="small"),
        text(857, 300, "schedule", cls="small"),
        box(45, 314, 245, 36, 'a_source = "b + 1"', fill="green_fill", stroke="green"),
        box(45, 357, 245, 36, 'b_source = "x"', fill="purple_fill", stroke="purple"),
        box(350, 330, 90, 44, "a", fill="green_fill", stroke="green"),
        box(490, 330, 90, 44, "b", fill="purple_fill", stroke="purple"),
        box(630, 330, 90, 44, "x", fill="blue_fill", stroke="blue"),
        arrow(440, 352, 490, 352, marker),
        arrow(580, 352, 630, 352, marker),
        box(775, 310, 165, 38, "cached [a, b]", fill="orange_fill", stroke="orange"),
        box(775, 360, 165, 38, "repaired [b, a]", fill="green_fill", stroke="green"),
        arrow(857, 348, 857, 360, marker),
        f'<rect x="25" y="423" width="950" height="155" rx="7" fill="{COLORS["panel"]}" stroke="{COLORS["border"]}"/>',
        text(45, 451, "Runtime tick n + 1", anchor="start", cls="label"),
        text(165, 475, "source values", cls="small"),
        text(535, 475, "active dependencies", cls="small"),
        text(857, 475, "schedule", cls="small"),
        box(45, 489, 245, 36, 'a_source = "x"', fill="green_fill", stroke="green"),
        box(
            45, 532, 245, 36, 'b_source = "a + 1"', fill="purple_fill", stroke="purple"
        ),
        box(350, 505, 90, 44, "b", fill="purple_fill", stroke="purple"),
        box(490, 505, 90, 44, "a", fill="green_fill", stroke="green"),
        box(630, 505, 90, 44, "x", fill="blue_fill", stroke="blue"),
        arrow(440, 527, 490, 527, marker),
        arrow(580, 527, 630, 527, marker),
        box(775, 485, 165, 38, "cached [b, a]", fill="orange_fill", stroke="orange"),
        box(775, 535, 165, 38, "repaired [a, b]", fill="green_fill", stroke="green"),
        arrow(857, 523, 857, 535, marker),
    ]
    return (
        svg(
            "Potential dependency graph, scope matrix, and runtime schedule repair",
            "The full dashed potential graph and compile-time matrix both show that dynamic stream a may read x or b and dynamic stream b may read x or a; self reads are excluded. Dependency arrows point from a stream to what it reads. At runtime tick n, source values activate a to b to x and repair cached order a then b to b then a. At the next runtime tick, new source values activate b to a to x and repair the cached order back to a then b. The ticks are consecutive logical input rows, not stages of one evaluation.",
            1000,
            600,
            "\n".join(parts),
            marker,
        ),
        "The full graph and matrix show the same compile-time permissions; each runtime tick activates a subset and repairs the cached dependency-first schedule when necessary.",
    )


def dynamic_history() -> tuple[str, str]:
    marker = "dynamic-history-arrow"
    parts = [
        text(
            24,
            27,
            "History follows temporal operator lifetime",
            anchor="start",
            cls="section",
        ),
        text(35, 62, "before replacement", anchor="start", cls="label"),
        text(35, 202, "after replacement", anchor="start", cls="label"),
    ]
    rows = [
        (82, "current x", "evaluator A", "local x[2] ring A", "z", "fixed z[1] ring"),
        (222, "current x", "evaluator B", "fresh x[2] ring B", "z", "same z[1] ring"),
    ]
    for y, current, evaluator, delay, output, downstream in rows:
        parts.extend(
            [
                box(35, y, 130, 52, current, fill="blue_fill", stroke="blue"),
                box(215, y, 140, 52, evaluator, fill="orange_fill", stroke="orange"),
                box(405, y, 175, 52, delay, fill="orange_fill", stroke="orange"),
                box(630, y, 80, 52, output, fill="green_fill", stroke="green"),
                box(760, y, 165, 52, downstream, fill="purple_fill", stroke="purple"),
                arrow(165, y + 26, 215, y + 26, marker),
                arrow(355, y + 26, 405, y + 26, marker),
                arrow(580, y + 26, 630, y + 26, marker),
                arrow(710, y + 26, 760, y + 26, marker),
            ]
        )
    parts.extend(
        [
            arrow(285, 134, 285, 222, marker, dashed=True),
            text(297, 182, "replace", anchor="start", cls="small"),
            arrow(842, 134, 842, 222, marker),
            text(854, 182, "continues", anchor="start", cls="small"),
        ]
    )
    return (
        svg(
            "Temporal state follows the lifetime of its operator",
            "Before replacement, current x flows through evaluator A and its local x indexed-by-two ring to stream z, then through a fixed downstream z indexed-by-one ring. After replacement, evaluator B owns a fresh local ring, while the same downstream ring continues to record values produced by z.",
            960,
            305,
            "\n".join(parts),
            marker,
        ),
        "Replacement creates fresh temporal state inside the active expression; temporal operators in the fixed surrounding specification continue.",
    )


def dynamic_lifecycle() -> tuple[str, str]:
    marker = "dynamic-arrow"
    parts = [
        text(
            24,
            27,
            "dynamic: source text selects an evaluator lifetime",
            anchor="start",
            cls="section",
        ),
        text(35, 62, "activation timeline", anchor="start", cls="label"),
    ]
    ticks = [
        (35, 'tick 10: "x + 1"', "activate A • history starts", "green"),
        (265, 'tick 11: "x + 1"', "equal text • reuse A", "green"),
        (495, 'tick 12: "x * 2"', "drop A • activate fresh B", "orange"),
        (725, 'tick 13: "x + 1"', "drop B • activate fresh C", "purple"),
    ]
    for x, label, note, color in ticks:
        parts.append(
            box(
                x,
                82,
                200,
                70,
                label,
                fill=f"{color}_fill",
                stroke=color,
                sublabel=note,
            )
        )
    for x1, x2 in [(235, 265), (465, 495), (695, 725)]:
        parts.append(arrow(x1, 117, x2, 117, marker))
    parts.extend(
        [
            divider(185, 960),
            text(35, 217, "per-activation delay history", anchor="start", cls="label"),
            box(
                35,
                237,
                270,
                68,
                "evaluator A",
                fill="green_fill",
                stroke="green",
                sublabel="rings begin tick 10; advance tick 11",
            ),
            box(
                345,
                237,
                270,
                68,
                "fresh evaluator B",
                fill="orange_fill",
                stroke="orange",
                sublabel="new rings begin tick 12",
            ),
            box(
                655,
                237,
                270,
                68,
                "fresh evaluator C",
                fill="purple_fill",
                stroke="purple",
                sublabel="new rings begin tick 13",
            ),
            text(
                480,
                345,
                "A and C have equal source text, but never share evaluator state or delay rings",
                cls="small",
            ),
            text(
                480,
                371,
                "Positive-delay history starts on each activation tick; earlier rows are not backfilled",
                cls="small",
            ),
        ]
    )
    return (
        svg(
            "Dynamic evaluator replacement and activation-scoped history",
            "Dynamic source text equal to the active definition reuses evaluator A. Changed text drops A and activates a fresh evaluator B. Returning later to the old text drops B and creates a fresh evaluator C rather than reviving A. Each evaluator owns separate per-delay rings, and positive-delay history starts on that evaluator's activation tick without backfill.",
            960,
            400,
            "\n".join(parts),
            marker,
        ),
        "Equal current source text reuses the active evaluator; each text change activates a fresh evaluator with new per-delay rings, even when the source later returns to an older string.",
    )


def defer_lifecycle() -> tuple[str, str]:
    marker = "defer-arrow"
    parts = [
        text(
            24,
            27,
            "defer keeps its first evaluator",
            anchor="start",
            cls="section",
        )
    ]
    ticks = [
        (35, "tick 10", "NoVal", "blue"),
        (265, "tick 11", '"x + 1"', "green"),
        (495, "tick 12", '"x * 2"', "orange"),
        (725, "tick 13", "Deferred", "purple"),
    ]
    for x, tick, source, color in ticks:
        parts.extend(
            [
                text(x + 100, 62, tick, cls="small"),
                box(
                    x,
                    75,
                    200,
                    54,
                    source,
                    fill=f"{color}_fill",
                    stroke=color,
                ),
            ]
        )
    for x1, x2 in [(235, 265), (465, 495), (695, 725)]:
        parts.append(arrow(x1, 102, x2, 102, marker))
    parts.extend(
        [
            divider(160, 960),
            text(35, 192, "active evaluator", anchor="start", cls="label"),
            box(
                265,
                218,
                660,
                70,
                'evaluator A: "x + 1"',
                fill="green_fill",
                stroke="green",
                sublabel="delay rings start here and remain continuous",
            ),
            arrow(365, 129, 365, 218, marker),
            arrow(595, 129, 595, 218, marker),
            arrow(825, 129, 825, 218, marker),
            text(375, 180, "activate", anchor="start", cls="small"),
            text(605, 180, "advance", anchor="start", cls="small"),
            text(835, 180, "advance", anchor="start", cls="small"),
        ]
    )
    return (
        svg(
            "Defer keeps the first active evaluator",
            "No evaluator is active before the first definition. The first string activates evaluator A and starts its delay rings. A later string and Deferred both advance A without replacing its program or state.",
            960,
            320,
            "\n".join(parts),
            marker,
        ),
        "The first string activates evaluator A; every later source advances that same evaluator and its continuous delay history.",
    )


DIAGRAMS = {
    "example-streams": example_streams,
    "pipeline": pipeline,
    "environment-layout": environment_layout,
    "history-retention": history_retention,
    "evaluation-graph": evaluation_graph,
    "specialization-overlay": specialization_overlay,
    "execution-layout": execution_layout,
    "lazy-if": lazy_if,
    "function-binding": function_binding,
    "function-call": function_call,
    "reconfiguration-points": reconfiguration_points,
    "dynamic-dependencies": dynamic_dependencies,
    "dynamic-history": dynamic_history,
    "dynamic-lifecycle": dynamic_lifecycle,
    "defer-lifecycle": defer_lifecycle,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check", action="store_true", help="fail if generated files differ"
    )
    args = parser.parse_args()
    expected_assets = {
        ASSET_DIR / f"{name}.svg": render()[0] + "\n"
        for name, render in DIAGRAMS.items()
    }
    stale = [
        str(path.relative_to(ROOT))
        for path, expected in expected_assets.items()
        if not path.exists() or path.read_text(encoding="utf-8") != expected
    ]
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
