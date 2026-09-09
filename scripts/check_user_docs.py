#!/usr/bin/env python3
"""Validate deterministic user-documentation contracts."""

from __future__ import annotations

import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parent.parent
DOC_ROOT = ROOT / "docs" / "src"
USER_SVGS = (
    DOC_ROOT / "assets" / "user" / "finite-monitor-ticks.svg",
    DOC_ROOT / "assets" / "user" / "mstlo-delayed-horizon.svg",
    DOC_ROOT / "assets" / "user" / "input-window-modes.svg",
    DOC_ROOT / "assets" / "user" / "dynamic-defer-user-ticks.svg",
    DOC_ROOT / "assets" / "user" / "reconfiguration-observation-ticks.svg",
)
MARKDOWN_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
FENCED_CODE = re.compile(r"```.*?```", re.DOTALL)
INLINE_CODE = re.compile(r"`[^`\n]*`")
URL_REFERENCE = re.compile(r"url\(#([^)]+)\)")
MIN_SVG_TEXT_SIZE = 11.0


def fail(message: str) -> None:
    raise RuntimeError(message)


def run(command: list[str]) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        fail(f"command failed ({' '.join(command)}):\n{result.stdout}{result.stderr}")
    return result.stdout


def markdown_files() -> list[Path]:
    return [ROOT / "README.md", *sorted(DOC_ROOT.rglob("*.md"))]


def check_markdown_links() -> None:
    failures: list[str] = []
    for source in markdown_files():
        text = source.read_text(encoding="utf-8")
        prose = FENCED_CODE.sub("", text)
        prose = INLINE_CODE.sub("", prose)
        for raw_target in MARKDOWN_LINK.findall(prose):
            target = raw_target.strip()
            if target.startswith("<") and target.endswith(">"):
                target = target[1:-1]
            target = target.split(maxsplit=1)[0]
            if not target or target.startswith(("#", "http://", "https://", "mailto:")):
                continue
            path_text = unquote(target.split("#", 1)[0].split("?", 1)[0])
            if not path_text:
                continue
            resolved = (source.parent / path_text).resolve()
            if not resolved.exists():
                failures.append(f"{source.relative_to(ROOT)} -> {target}")
    if failures:
        fail("broken local Markdown targets:\n" + "\n".join(failures))


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def check_user_svgs() -> None:
    all_ids: dict[str, Path] = {}
    failures: list[str] = []

    for path in USER_SVGS:
        if not path.is_file():
            failures.append(f"missing SVG: {path.relative_to(ROOT)}")
            continue
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError as error:
            failures.append(f"invalid XML in {path.relative_to(ROOT)}: {error}")
            continue

        file_ids: set[str] = set()
        references: set[str] = set()
        for element in root.iter():
            element_id = element.get("id")
            if element_id:
                if element_id in file_ids:
                    failures.append(
                        f"duplicate ID `{element_id}` in {path.relative_to(ROOT)}"
                    )
                file_ids.add(element_id)
                previous = all_ids.get(element_id)
                if previous is not None:
                    failures.append(
                        f"cross-file duplicate ID `{element_id}` in "
                        f"{previous.relative_to(ROOT)} and {path.relative_to(ROOT)}"
                    )
                all_ids[element_id] = path

            for attribute, value in element.attrib.items():
                references.update(URL_REFERENCE.findall(value))
                if local_name(attribute) == "href" and value.startswith("#"):
                    references.add(value[1:])

            if local_name(element.tag) == "text":
                size = element.get("font-size")
                if size is not None:
                    try:
                        numeric_size = float(size.removesuffix("px"))
                    except ValueError:
                        failures.append(
                            f"non-numeric text size `{size}` in {path.relative_to(ROOT)}"
                        )
                    else:
                        if numeric_size < MIN_SVG_TEXT_SIZE:
                            failures.append(
                                f"text size {numeric_size:g} below {MIN_SVG_TEXT_SIZE:g} in "
                                f"{path.relative_to(ROOT)}"
                            )

        missing_references = sorted(references - file_ids)
        if missing_references:
            failures.append(
                f"missing local SVG references in {path.relative_to(ROOT)}: "
                + ", ".join(missing_references)
            )

    if failures:
        fail("SVG integrity failures:\n" + "\n".join(failures))


def check_generated_cli_reference() -> None:
    run(["./scripts/check_cli_reference.sh", "--check"])


def check_finite_examples() -> None:
    examples = (
        (
            [
                "cargo",
                "run",
                "--quiet",
                "--bin",
                "trustworthiness_checker",
                "--",
                "examples/simple_add.dsrv",
                "--input-file",
                "examples/simple_add.input",
                "--output-stdout",
            ],
            "z[0] = Int(3)\nz[1] = Int(7)\n",
        ),
        (
            [
                "cargo",
                "run",
                "--quiet",
                "--bin",
                "trustworthiness_checker",
                "--",
                "examples/counter.dsrv",
                "--input-file",
                "examples/counter.input",
                "--output-stdout",
            ],
            "z[0] = Int(1)\nz[1] = Int(2)\nz[2] = Int(3)\nz[3] = Int(4)\n",
        ),
        (
            [
                "cargo",
                "run",
                "--quiet",
                "--bin",
                "trustworthiness_checker",
                "--",
                "examples/counter_threshold.dsrv",
                "--input-file",
                "examples/counter.input",
                "--output-stdout",
            ],
            (
                "below_limit[0] = Bool(true)\n"
                "below_limit[1] = Bool(true)\n"
                "below_limit[2] = Bool(false)\n"
                "below_limit[3] = Bool(false)\n"
            ),
        ),
        (
            [
                "cargo",
                "run",
                "--quiet",
                "--bin",
                "trustworthiness_checker",
                "--",
                "examples/simple_stl.mstlo",
                "--input-file",
                "examples/simple_stl.input",
                "--output-stdout",
                "--language",
                "mstlo",
                "--semantics",
                "delayed-qualitative",
                "--execution-policy",
                "synchronous",
                "--mstlo-synchronization",
                "none",
            ],
            (
                'always_x[0] = {"time":0,"value":true}\n'
                'combo[1] = {"time":0,"value":true}\n'
                'always_x[2] = {"time":1000,"value":false}\n'
                'combo[3] = {"time":1000,"value":true}\n'
                'always_x[4] = {"time":2000,"value":false}\n'
                'combo[5] = {"time":2000,"value":true}\n'
                'always_x[6] = {"time":3000,"value":false}\n'
                'combo[7] = {"time":3000,"value":true}\n'
            ),
        ),
        (
            [
                "cargo",
                "run",
                "--quiet",
                "--bin",
                "trustworthiness_checker",
                "--",
                "examples/simple_stl_threshold.mstlo",
                "--input-file",
                "examples/simple_stl.input",
                "--output-stdout",
                "--language",
                "mstlo",
                "--semantics",
                "delayed-quantitative",
                "--execution-policy",
                "synchronous",
                "--mstlo-synchronization",
                "none",
                "--mstlo-vars",
                "threshold=3",
            ],
            (
                'always_x[0] = {"time":0,"value":1.0}\n'
                'always_x[1] = {"time":1000,"value":-1.0}\n'
                'always_x[2] = {"time":2000,"value":-1.0}\n'
                'always_x[3] = {"time":3000,"value":-1.0}\n'
            ),
        ),
    )
    for command, expected in examples:
        actual = run(command)
        if actual != expected:
            fail(
                f"unexpected stdout from {' '.join(command)}:\n"
                f"expected:\n{expected}actual:\n{actual}"
            )


def main() -> int:
    checks = (
        ("Markdown links", check_markdown_links),
        ("user SVGs", check_user_svgs),
        ("generated CLI reference", check_generated_cli_reference),
        ("ordinary finite examples", check_finite_examples),
    )
    try:
        for label, check in checks:
            check()
            print(f"ok: {label}")
    except RuntimeError as error:
        print(f"check_user_docs: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
