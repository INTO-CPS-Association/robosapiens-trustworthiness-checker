#!/usr/bin/env python3
"""Validate the parts of the documentation build that fail silently.

Figures are pulled into Markdown with `{{#include ...svg}}`, so a figure is raw
HTML embedded in a Markdown document. Two CommonMark rules therefore decide whether a
figure renders at all, and breaking either one produces a page that still builds,
still passes `mdbook build`, and still looks plausible in a diff:

  * An HTML block starts only on a *complete* open tag followed by nothing but
    whitespace. An `<svg ...>` tag broken across lines opens a paragraph
    instead; `</p>` then closes the `<svg>` early and the browser shows an empty
    box followed by the figure's `<desc>` and every text label run together as
    one paragraph.
  * An HTML block ends at the first blank line. A blank line inside a figure
    truncates it; a missing blank line *after* the include swallows the caption
    that follows, which then shows its `**bold**` and `` `code` `` as literal
    text.

A third failure has the same shape: `{{#include file.rs:anchor}}` renders an empty
code block when the anchor is missing, and mdBook still exits 0, so a renamed region
publishes a blank example with nothing to notice.

These are only directly observable in the built HTML, so --book is the check that
matters most; the source checks exist to name the cause. Mermaid diagrams and the
Mermaid initialiser are checked too, since both depend on the shared figure palette.

    python3 scripts/validate_docs.py           # sources only
    python3 scripts/validate_docs.py --book    # sources + docs/book
    python3 scripts/validate_docs.py --list    # what would be checked
"""

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "docs" / "src"
BOOK = ROOT / "docs" / "book"

INCLUDE = re.compile(r"\{\{#include\s+([^}]+?)\}\}")
SVG_NS = "{http://www.w3.org/2000/svg}"

# A monospace face advances 0.6 em per character. Measured across every label in
# docs/src/assets the ratio is 0.6000 with no spread, so a monospace label's width
# can be computed rather than approximated.
MONO_ADVANCE_EM = 0.6


@dataclass
class Report:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def error(self, where: str, message: str) -> None:
        self.errors.append(f"{where}: {message}")

    def warn(self, where: str, message: str) -> None:
        self.warnings.append(f"{where}: {message}")


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def svg_includes() -> list[tuple[Path, int, str, Path]]:
    """Every `{{#include ...svg}}` as (markdown file, line number, raw path, target)."""
    found = []
    for md in sorted(SRC.rglob("*.md")):
        for number, line in enumerate(md.read_text(encoding="utf-8").splitlines(), 1):
            for raw in INCLUDE.findall(line):
                raw = raw.strip()
                # `{{#include file:anchor}}` splices a named region of a file and is
                # not a figure; only a whole-file SVG include embeds a drawing.
                if not raw.endswith(".svg"):
                    continue
                found.append((md, number, raw, (md.parent / raw).resolve()))
    return found


# --------------------------------------------------------------------------
# source checks
# --------------------------------------------------------------------------


def check_include_sites(report: Report) -> None:
    """The include line itself: target resolves, stands alone, and a blank line follows."""
    for md, number, raw, target in svg_includes():
        where = f"{rel(md)}:{number}"
        lines = md.read_text(encoding="utf-8").splitlines()

        if not target.exists():
            report.error(where, f"include target does not exist: {raw}")
            continue
        if lines[number - 1].strip() != "{{#include " + raw + "}}":
            report.error(where, "the include must stand alone on its line to open an HTML block")
        following = lines[number] if number < len(lines) else ""
        if following.strip():
            report.error(
                where,
                "no blank line after the include, so the next paragraph is absorbed into the "
                "figure's HTML block and renders with its Markdown markup visible",
            )


def check_svg_source(
    path: Path, report: Report, *, inline: bool = False
) -> ET.Element | None:
    """Check an SVG's structure and, when inlined, its CommonMark formatting."""
    where = rel(path)
    text = path.read_text(encoding="utf-8")

    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        report.error(where, f"not well-formed XML: {exc}")
        return None

    if root.tag.rsplit("}", 1)[-1] != "svg":
        report.error(where, "root element is not <svg>")
        return None

    if inline:
        opening = re.match(r"<svg\b[^>]*>", text, re.S)
        if not opening:
            report.error(
                where,
                "does not begin with an <svg> open tag, so CommonMark cannot open an HTML block",
            )
            return None
        if "\n" in opening.group(0):
            report.error(
                where,
                "the <svg> open tag spans multiple lines, so CommonMark opens a paragraph instead "
                "of an HTML block and the figure does not render. Join the tag onto one line",
            )

        for number, line in enumerate(text.splitlines(), 1):
            if not line.strip():
                report.error(
                    f"{where}:{number}",
                    "blank line inside the figure; it ends the HTML block and the remainder is "
                    "parsed as Markdown",
                )

    if not root.get("viewBox"):
        report.error(where, "no viewBox, so the figure cannot scale to the page width")

    return root


def viewbox(root: ET.Element) -> tuple[float, float] | None:
    try:
        _, _, width, height = (float(value) for value in root.get("viewBox", "").split())
    except ValueError:
        return None
    return width, height


def check_shapes(path: Path, root: ET.Element, report: Report) -> None:
    """Rectangles and lines must sit inside the viewBox, or they are clipped away."""
    box = viewbox(root)
    if box is None:
        return
    width, height = box

    def number(element: ET.Element, name: str) -> float:
        try:
            return float(element.get(name, 0))
        except (TypeError, ValueError):
            return 0.0

    for element in root.iter():
        tag = element.tag.replace(SVG_NS, "")
        if tag == "rect":
            right = number(element, "x") + number(element, "width")
            bottom = number(element, "y") + number(element, "height")
            left, top = number(element, "x"), number(element, "y")
        elif tag == "line":
            left = min(number(element, "x1"), number(element, "x2"))
            right = max(number(element, "x1"), number(element, "x2"))
            top = min(number(element, "y1"), number(element, "y2"))
            bottom = max(number(element, "y1"), number(element, "y2"))
        else:
            continue
        if right > width + 0.5 or bottom > height + 0.5 or left < -0.5 or top < -0.5:
            report.error(
                rel(path),
                f"<{tag}> spans ({left:.0f},{top:.0f})-({right:.0f},{bottom:.0f}), outside the "
                f"{width:.0f}x{height:.0f} viewBox",
            )


def check_monospace_labels(path: Path, root: ET.Element, report: Report) -> None:
    """Monospace labels have an exact advance width, so their clipping is decidable.

    Proportional text is deliberately not checked. Its width depends on the font the
    reader's browser resolves `system-ui` to, and a per-character bound calibrated on
    the widest short string (0.616 em here) overestimates a long string of narrow
    characters badly enough to flag figures that render correctly. Clipped
    proportional labels are caught by reviewing a render, not by this script.
    """
    box = viewbox(root)
    if box is None:
        return
    width, _ = box

    for element in root.iter(SVG_NS + "text"):
        family = element.get("font-family") or ""
        if "mono" not in family:
            continue
        content = "".join(element.itertext()).strip()
        if not content:
            continue
        try:
            size = float(element.get("font-size", 12))
            x = float(element.get("x", 0))
        except (TypeError, ValueError):
            continue
        span = len(content) * MONO_ADVANCE_EM * size
        anchor = element.get("text-anchor", "start")
        left = x - span / 2 if anchor == "middle" else x - span if anchor == "end" else x
        right = left + span
        if right > width + 1 or left < -1:
            report.error(
                rel(path),
                f"monospace label {content[:44]!r} spans x={left:.0f}-{right:.0f} in a "
                f"{width:.0f}-wide viewBox and is clipped",
            )


def check_mermaid_initialiser(report: Report) -> None:
    """`mdbook-mermaid install` overwrites the initialiser, so the build copies ours over it.

    docs/mermaid-init.js is gitignored and regenerated, and the installer re-adds it to
    book.toml's additional-js every time it runs. The build therefore always loads that
    file, and the step that makes it ours is a copy from the tracked source. If the copy
    is skipped, diagrams silently fall back to Mermaid's own palette.
    """
    generated = ROOT / "docs" / "mermaid-init.js"
    tracked = ROOT / "docs" / "theme" / "mermaid-theme.js"
    if not tracked.exists():
        report.error(rel(tracked), "missing; it is the tracked source of the Mermaid initialiser")
        return
    if not generated.exists():
        report.warn(rel(generated), "absent; run `mdbook-mermaid install docs`")
        return
    if generated.read_text(encoding="utf-8") != tracked.read_text(encoding="utf-8"):
        report.error(
            rel(generated),
            "differs from the tracked theme/mermaid-theme.js, so diagrams will not use the "
            "figure palette. Run: cp docs/theme/mermaid-theme.js docs/mermaid-init.js",
        )


CODE_INCLUDE = re.compile(r"\{\{#include\s+([^}:\s]+\.rs):([A-Za-z0-9_]+)\s*\}\}")


def check_code_includes(report: Report) -> int:
    """Anchored code includes must name a region that exists.

    mdBook renders an empty code block for a missing anchor and still exits 0, so a
    renamed region publishes a blank example with no build failure to notice.
    """
    checked = 0
    for md in sorted(SRC.rglob("*.md")):
        text = md.read_text(encoding="utf-8")
        for number, line in enumerate(text.splitlines(), 1):
            for target, anchor in CODE_INCLUDE.findall(line):
                where = f"{rel(md)}:{number}"
                source = (md.parent / target).resolve()
                if not source.exists():
                    report.error(where, f"include target does not exist: {target}")
                    continue
                body = source.read_text(encoding="utf-8")
                for marker in (f"ANCHOR: {anchor}", f"ANCHOR_END: {anchor}"):
                    if marker not in body:
                        report.error(
                            where,
                            f"{target} has no `{marker}`; mdBook will render an empty code "
                            "block and still succeed",
                        )
                        break
                else:
                    checked += 1
    return checked


MERMAID_BLOCK = re.compile(r"```mermaid\n(.*?)```", re.S)
MERMAID_COLOUR = re.compile(r"(?:fill|stroke|color)\s*:\s*(#[0-9a-fA-F]{3,8})")


def check_mermaid(report: Report) -> int:
    """Mermaid diagrams share the figures' palette, which a literal colour would break.

    `theme/mermaid-init.js` maps the `--fig-*` tokens onto Mermaid's theme variables so
    a diagram and a drawn figure match in every mdBook theme. A colour written into the
    diagram source is fixed instead, so it survives only one of the two grounds.
    """
    blocks = 0
    for md in sorted(SRC.rglob("*.md")):
        text = md.read_text(encoding="utf-8")
        for index, block in enumerate(MERMAID_BLOCK.findall(text), 1):
            blocks += 1
            where = f"{rel(md)} (mermaid block {index})"
            for colour in MERMAID_COLOUR.findall(block):
                report.error(
                    where,
                    f"literal colour {colour}; it cannot follow the theme. Use the diagram's "
                    "default styling, which is driven by the shared figure palette",
                )
            if "accTitle" not in block or "accDescr" not in block:
                report.warn(where, "no accTitle/accDescr, so the diagram has no accessible description")
    return blocks


# --------------------------------------------------------------------------
# built-book checks
# --------------------------------------------------------------------------


def check_book(report: Report) -> int:
    """The rendered HTML, where a mis-embedded figure actually becomes visible."""
    if not BOOK.exists():
        report.error(rel(BOOK), "not built; run `mdbook build` in docs/ first")
        return 0

    verified = 0
    flagged_pages: set[str] = set()
    for md, _number, _raw, target in svg_includes():
        page = BOOK / md.relative_to(SRC).with_suffix(".html")
        where = rel(page)
        if not page.exists():
            report.warn(where, f"page absent from the built book (is {rel(md)} in SUMMARY.md?)")
            continue
        html = page.read_text(encoding="utf-8")

        if "<p><svg" in html and where not in flagged_pages:
            flagged_pages.add(where)
            report.error(
                where,
                "a figure is wrapped in <p>; the </p> closes its <svg> and it will not render",
            )

        # A figure survived embedding only if its <desc> is still inside the <svg> that
        # opened before it. Counting tags is not enough: when the open tag lands inside a
        # paragraph the `<svg` is present but a `</p>` between them has already closed it,
        # which is the case this check exists for.
        source = target.read_text(encoding="utf-8")
        desc = re.search(r"<desc[^>]*>(.{0,60})", source, re.S)
        if desc is None:
            report.warn(rel(target), "no <desc>, so the figure has no accessible description")
            continue
        index = html.find(desc.group(1).strip())
        if index < 0:
            report.error(where, f"{target.name} does not appear on the page")
            continue

        before = html[:index]
        opened = before.rfind("<svg")
        if opened < 0 or "</svg>" in before[opened:]:
            report.error(
                where,
                f"{target.name}: its <desc> sits outside any <svg>, so the figure renders as "
                "page text instead of a drawing",
            )
            continue
        if "</p>" in before[opened:] or "<p>" in before[opened:]:
            report.error(
                where,
                f"{target.name}: a paragraph boundary falls inside the figure, closing its "
                "<svg> early; the drawing is emitted as page text",
            )
            continue
        verified += 1

    # An anchored include that resolved to nothing leaves an empty code block.
    for page in sorted(BOOK.rglob("*.html")):
        text = page.read_text(encoding="utf-8")
        for match in re.finditer(r'<code class="language-\w[^"]*">(.*?)</code>', text, re.S):
            stripped = re.sub(r"<[^>]+>", "", match.group(1)).strip()
            if stripped in ("", "#![allow(unused)]\nfn main() {\n}"):
                report.error(
                    rel(page),
                    "an empty code block; an anchored include resolved to nothing",
                )
                break

    # A caption absorbed by a figure's HTML block keeps its Markdown markup verbatim.
    for page in sorted(BOOK.rglob("*.html")):
        for number, line in enumerate(page.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith("**"):
                report.error(
                    f"{rel(page)}:{number}",
                    f"literal Markdown in the rendered page ({line.strip()[:48]!r}); a paragraph "
                    "was absorbed into a figure's HTML block",
                )
                break
    return verified


# --------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--book", action="store_true",
                        help="also validate the built HTML under docs/book")
    parser.add_argument("--list", action="store_true",
                        help="list the figures and include sites, then exit")
    args = parser.parse_args()

    assets = sorted((SRC / "assets").rglob("*.svg"))
    includes = svg_includes()
    inline_targets = {target for _md, _number, _raw, target in includes}

    if args.list:
        for path in assets:
            print(f"figure  {rel(path)}")
        for md, number, raw, _target in includes:
            print(f"include {rel(md)}:{number}  {raw}")
        return 0

    if not assets:
        print("no SVG assets found under docs/src/assets", file=sys.stderr)
        return 1

    report = Report()
    for path in assets:
        root = check_svg_source(path, report, inline=path.resolve() in inline_targets)
        if root is not None:
            check_shapes(path, root, report)
            check_monospace_labels(path, root, report)
    check_include_sites(report)
    mermaid = check_mermaid(report)
    code = check_code_includes(report)
    check_mermaid_initialiser(report)

    verified = check_book(report) if args.book else 0
    if args.book and includes and verified == 0:
        # Every page absent is reported per page as a warning, so without this the run
        # would print "ok" having verified nothing at all. A build still in progress
        # looks exactly like this.
        report.error(
            rel(BOOK),
            "no figure could be verified; docs/book is missing, stale, or still being written",
        )

    for warning in report.warnings:
        print(f"warning: {warning}")
    for error in report.errors:
        print(f"error: {error}", file=sys.stderr)

    if report.errors:
        print(f"\n{len(report.errors)} problem(s) across {len(assets)} figures", file=sys.stderr)
        return 1

    summary = (
        f"ok: {len(assets)} figures, {len(includes)} figure includes, "
        f"{code} code includes, {mermaid} mermaid diagrams"
    )
    if args.book:
        summary += f", {verified} embedded correctly in the built book"
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
