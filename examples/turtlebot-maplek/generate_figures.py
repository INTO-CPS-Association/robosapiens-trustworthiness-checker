"""Draw tutorial timelines from the synthetic journals; use --check to detect drift.

The diagrams select evidence, not every record field. Horizontal coordinates
are either selected native record ticks or the explicitly labelled supplied time.
No robot measurement or physical recovery result is inferred.
"""

import argparse
import html
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from diagram_theme import SVG_THEME_BLOCK

OUTPUT = ROOT / "docs/src/assets/user/turtlebot-maplek"


def journal(name):
    return [json.loads(line) for line in
            (ROOT / f"examples/turtlebot-maplek/native/{name}.jsonl").read_text().splitlines()]


class Figure:
    def __init__(self, slug, title, description, width, height):
        self.slug = "turtlebot-" + slug
        self.width = width
        self.parts = [
            f'<svg class="tc-figure" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" role="img" aria-labelledby="{self.slug}-title {self.slug}-desc" style="width:100%; max-width:{width}px; height:auto;">',
            f'  <title id="{self.slug}-title">{html.escape(title)}</title>',
            f'  <desc id="{self.slug}-desc">{html.escape(description)}</desc>',
            SVG_THEME_BLOCK,
            f'  <rect x="0.75" y="0.75" width="{width - 1.5}" height="{height - 1.5}" rx="6" fill="var(--fig-bg)" stroke="var(--fig-edge)" stroke-width="1.5" />',
        ]
        self.text(20, 30, title, size=15, bold=True)

    def text(self, x, y, value, size=12, anchor="start", bold=False, secondary=False):
        token = "ink-2" if secondary else "ink"
        weight = ' font-weight="600"' if bold else ""
        self.parts.append(
            f'  <text x="{x:.2f}" y="{y}" text-anchor="{anchor}" font-family="system-ui, sans-serif" font-size="{size}"{weight} fill="var(--fig-{token})">{html.escape(str(value))}</text>'
        )

    def line(self, x1, y1, x2, y2, dash=False, token="line", width=1.5):
        dashed = ' stroke-dasharray="5 4"' if dash else ""
        self.parts.append(f'  <path d="M {x1:.2f} {y1} L {x2:.2f} {y2}" stroke="var(--fig-{token})" stroke-width="{width}"{dashed} fill="none" />')

    def dot(self, x, y, token="blue-line", open_dot=False):
        fill = "bg" if open_dot else token
        self.parts.append(f'  <circle cx="{x:.2f}" cy="{y}" r="4" stroke="var(--fig-{token})" fill="var(--fig-{fill})" stroke-width="1.5" />')

    def cell(self, x, y, value, token="panel", width=60):
        border = "panel-line" if token == "panel" else token + "-line"
        self.parts.append(f'  <rect x="{x - width / 2:.2f}" y="{y - 19}" width="{width}" height="30" rx="4" fill="var(--fig-{token})" stroke="var(--fig-{border})" stroke-width="1.5" />')
        self.text(x, y, value, anchor="middle", bold=True)

    def axis(self, ticks, y=86, start=190, end=None, label="Journal tick"):
        end = self.width - 38 if end is None else end
        self.line(start - 15, y, end + 15, y)
        positions = {}
        for i, tick in enumerate(ticks):
            x = start + i * (end - start) / (len(ticks) - 1)
            positions[tick] = x
            self.line(x, y - 5, x, y + 5)
            self.text(x, y - 14, tick, anchor="middle")
        self.text(20, y - 14, label, bold=True)
        return positions

    def finish(self):
        return "\n".join(self.parts + ["</svg>"]) + "\n"


def planning():
    def times(name):
        rows = journal(name)
        at = lambda role, step: next(r["payload"]["arrival"]["journal_time"] for r in rows if r["$tag"] == "Phase"
                                     and r["payload"]["activation"]["role"] == role
                                     and r["payload"]["step"]["$tag"] == step)
        return at("Analysis", "End"), at("Plan", "Start"), at("Plan", "End")
    (a_open, a_start, a_end), (s_open, s_start, s_end) = times("accepted"), times("slow")
    f = Figure("planning-traces", "A fast callback can follow a slow planning response",
               f"Two traces share the same journal-time scale. In accepted, planning runs from anomaly at {a_open:g} seconds to Generated at {a_end:g}, while the Plan callback runs from {a_start:g} to {a_end:g}. In slow, planning runs from {s_open:g} to {s_end:g}, while the callback runs from {s_start:g} to {s_end:g}. Dashed marks are deadlines: 2 seconds after anomaly, or 1 second after callback Start. Only the slow planning outcome exceeds its budget.", 660, 480)
    x = lambda t: 200 + (t - 0.5) * 97
    f.text(20, 53, "Solid span: observed interval · dashed mark: deadline", secondary=True)
    for base, name, label in [(82, "accepted", "Accepted adaptation"), (284, "slow", "Delayed planning")]:
        opened, start, end = times(name)
        f.text(20, base + 12, label, size=14, bold=True)
        f.line(x(0.5), base + 37, x(5), base + 37)
        for t in [1, 2, 3, 4, 5]:
            f.line(x(t), base + 33, x(t), base + 41)
            f.text(x(t), base + 29, t, anchor="middle", secondary=True)
        for y, title, begin, budget in [(base + 77, "Planning outcome", opened, 2.0),
                                         (base + 147, "Plan callback", start, 1.0)]:
            fail = end - begin > budget
            color = "orange-line" if fail else "blue-line"
            f.text(20, y - 4, title, bold=True)
            f.text(20, y + 16, f"{end - begin:.1f} s / {budget:g} s budget", secondary=True)
            f.line(x(begin), y, x(end), y, token=color, width=3)
            f.dot(x(begin), y, token=color, open_dot=True)
            f.dot(x(end), y, token=color)
            f.line(x(begin + budget), y - 15, x(begin + budget), y + 15, dash=True)
            f.text(x(begin), y + 29, f"{begin:g}", anchor="middle", secondary=True)
            f.text(x(end), y - 13, f"{end:g}", anchor="middle", secondary=True)
    f.text(200, 465, "Supplied journal time (seconds)", secondary=True)
    return f.finish()


def step_of(row):
    return row["payload"]["step"]["$tag"] if row["$tag"] == "Phase" else row["$tag"]


def where(rows, role, step, key=None):
    """The first tick of `role`'s `step`, optionally accessing `key`."""
    return next(i for i, r in enumerate(rows) if r["$tag"] == "Phase"
                and r["payload"]["activation"]["role"] == role and step_of(r) == step
                and (key is None or r["payload"]["step"]["payload"].get("key") == key))


def freshness():
    rows = journal("same-value-update")
    read = where(rows, "Plan", "Read", "BoolLidarMasks")
    store = next(i for i, r in enumerate(rows) if r["$tag"] == "Knowledge"
                 and r["payload"]["version"]["written_by"]["$tag"] == "Node")
    generated = where(rows, "Plan", "End")
    old = rows[read]["payload"]["knowledge"]["payload"]["global_write_sequence"]
    new = rows[store]["payload"]["version"]["ref"]["global_write_sequence"]
    assert read < store < generated
    assert rows[generated]["payload"]["step"]["payload"]["outcome"]["payload"]["$tag"] == "Generated"
    v_old, v_new = f"v{old}", f"v{new}"
    ticks = [i for i in range(read, generated + 1)
             if rows[i]["$tag"] == "Phase" or i == store]
    f = Figure("freshness-trace", "Equal mask values can hide a stale Read",
               f"Selected native record ticks {read} through {generated} of same-value-update. Plan retains mask version {v_old}, the version it read at tick {read}, across all ticks. Newest observed mask is {v_old} until another writer stores {v_new} at tick {store}, and {v_new} from then on. At Generated on tick {generated}, payload equality passes while exact version equality fails. Blank verdict positions have no Generated decision.", 740, 346)
    xs = f.axis(ticks)
    for y, label in [(131, "Plan's retained Read"), (186, "Newest observed mask")]:
        f.text(20, y, label, bold=True)
        for tick, x in xs.items():
            newer = y == 186 and tick >= store
            f.cell(x, y, v_new if newer else v_old, "orange" if newer else "blue")
    f.text(20, 237, "Version check", bold=True)
    f.text(20, 253, "plan_read_newest_masks", size=10, secondary=True)
    f.cell(xs[generated], 241, "false", "orange")
    f.text(20, 282, "Selected evidence", bold=True)
    for tick, value in [(read, f"Read {v_old}"), (store, f"Store {v_new}"), (generated, "Generated")]:
        f.text(xs[tick], 282, value, anchor="middle", size=11)
    f.text(20, 321, f"{v_old} and {v_new} have the same payload. Only tick {generated} evaluates the generation check.", secondary=True)
    return f.finish()


def effect():
    rows = journal("wrong-effect")
    accepted = where(rows, "Legitimate", "End")
    start = where(rows, "Execute", "Start")
    request = where(rows, "Execute", "ServiceStarted")
    delivered = where(rows, "Execute", "ServiceCompleted")
    written = where(rows, "Execute", "Write", "SpinConfig")
    applied = where(rows, "Execute", "End")
    assert rows[accepted]["payload"]["step"]["payload"]["outcome"]["payload"]["$tag"] == "Accepted"
    assert rows[delivered]["payload"]["step"]["payload"]["outcome"]["$tag"] == "Delivered"
    effect_ref = rows[written]["payload"]["knowledge"]["payload"]
    effect_value = next(r["payload"]["version"]["value"]["payload"] for r in rows
                        if r["$tag"] == "Knowledge"
                        and r["payload"]["version"]["ref"] == effect_ref)
    assert effect_value["commands"][0]["angleVelocity"] == 0.2
    assert rows[applied]["payload"]["step"]["payload"]["outcome"]["payload"]["$tag"] == "Applied"
    f = Figure("wrong-effect-trace", "Delivery succeeds, but Execute reports the wrong spin",
               f"Selected native wrong-effect ticks {accepted} through {applied}. Legitimate accepts the 0.4 radian per second plan at {accepted}, Execute starts at {start} and its service request at {request}, and Delivered arrives at {delivered}. Execute writes a configuration with 0.2 radian per second at {written}, and Applied cites that effect at {applied}. The command comparison fails at tick {applied} despite successful delivery. Both commands last eight seconds.", 740, 335)
    xs = f.axis([i for i in range(accepted, applied + 1)
                 if rows[i]["$tag"] == "Phase"], start=194)
    f.text(20, 125, "Accepted plan", bold=True)
    f.text(20, 143, "rad/s; duration 8 s", size=11, secondary=True)
    for x in xs.values():
        f.cell(x, 129, "0.4", "blue", width=56)
    f.text(20, 185, "Delivery outcome", bold=True)
    f.cell(xs[delivered], 185, "Delivered", "green", width=73)
    f.text(20, 237, "Reported effect", bold=True)
    f.cell(xs[applied], 237, "0.2", "orange", width=56)
    f.text(20, 283, "Effect check", bold=True)
    f.text(20, 299, "applied_config_matches_plan", size=10, secondary=True)
    f.cell(xs[applied], 287, "false", "orange", width=56)
    f.text(xs[accepted], 165, "Accepted", anchor="middle", size=11, secondary=True)
    f.text(xs[start], 214, "Start", anchor="middle", size=11, secondary=True)
    f.text(xs[request], 214, "Request", anchor="middle", size=11, secondary=True)
    f.text(xs[applied], 214, "Applied", anchor="middle", size=11, secondary=True)
    f.text(20, 318, "Only the Applied outcome evaluates the effect comparison. Blank cells are no event.", secondary=True)
    return f.finish()


def silence():
    rows = journal("pending-service")
    last = len(rows) - 1
    start = rows[last]["payload"]["arrival"]["journal_time"]
    assert step_of(rows[last]) == "ServiceStarted"
    config = json.loads((ROOT / "examples/turtlebot-maplek/pending-service-lifecycle-input.json5").read_text())
    clocks = [sample["time"] for sample in config["sources"]["journal"]["clock_samples"]]
    assert clocks == [4.0, 5.0]
    ages = [0.0] + [round(t - start, 1) for t in clocks]
    ticks = [last, last + 1, last + 2]
    f = Figure("silent-service-trace", "Clock-only ticks reveal an overdue request",
               f"Observation ticks {ticks[0]} through {ticks[2]}. The service starts at journal time {start:g} seconds on tick {ticks[0]}. No further journal records arrive; clock samples at 4 and 5 seconds, released after that record, create ticks {ticks[1]} and {ticks[2]}. Request age increases from 0 to {ages[1]:g} and {ages[2]:g} seconds. The request remains pending, apply_spin_on_time changes from true to false, and no end of the request is observed. Equal tick spacing does not imply equal elapsed time.", 640, 355)
    xs = f.axis(ticks, start=245, end=580, label="Observation tick")
    for y, label, values in [
        (130, "Input", ["Service Start", "Clock only", "Clock only"]),
        (175, "Time (seconds)", [f"{start:g}", "4.0", "5.0"]),
        (220, "Request age (seconds)", [f"{age:.1f}" for age in ages]),
        (265, "apply_spin_pending", ["true", "true", "true"]),
        (310, "apply_spin_on_time", ["true", "false", "false"]),
    ]:
        f.text(20, y, label, bold=True)
        for i, tick in enumerate(xs):
            token = "orange" if label == "apply_spin_on_time" and i else "panel"
            f.cell(xs[tick], y, values[i], token, width=100 if label == "Input" else 64)
    f.text(20, 342, "No ServiceCompleted or owner End is observed; the request remains open.", secondary=True)
    return f.finish()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    figures = {"planning-traces.svg": planning(), "freshness-trace.svg": freshness(),
               "wrong-effect-trace.svg": effect(), "silent-service-trace.svg": silence()}
    for name, source in figures.items():
        path = OUTPUT / name
        if args.check:
            if not path.exists() or path.read_text() != source:
                raise SystemExit(f"Stale tutorial figure: {path.relative_to(ROOT)}")
        else:
            path.write_text(source)
    print(f"{'Checked' if args.check else 'Generated'} {len(figures)} tutorial timelines.")


if __name__ == "__main__":
    main()
