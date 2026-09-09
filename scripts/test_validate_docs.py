import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import validate_docs
from diagram_theme import SVG_THEME_BLOCK


class CheckSvgSourceTests(unittest.TestCase):
    def write_svg(self, directory: str, content: str) -> Path:
        path = Path(directory) / "figure.svg"
        path.write_text(content, encoding="utf-8")
        return path

    def test_commonmark_formatting_only_applies_to_inline_svg(self) -> None:
        content = """<svg xmlns="http://www.w3.org/2000/svg"
 viewBox="0 0 10 10">

<rect width="10" height="10"/>
</svg>
"""
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_svg(directory, content)

            standalone_report = validate_docs.Report()
            root = validate_docs.check_svg_source(path, standalone_report)
            self.assertIsNotNone(root)
            self.assertEqual([], standalone_report.errors)

            inline_report = validate_docs.Report()
            root = validate_docs.check_svg_source(path, inline_report, inline=True)
            self.assertIsNotNone(root)
            self.assertTrue(
                any("open tag spans multiple lines" in error for error in inline_report.errors)
            )
            self.assertTrue(
                any("blank line inside the figure" in error for error in inline_report.errors)
            )

    def test_bad_xml_is_rejected_for_inline_and_standalone_svg(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_svg(directory, '<svg viewBox="0 0 10 10">')

            for inline in (False, True):
                with self.subTest(inline=inline):
                    report = validate_docs.Report()
                    root = validate_docs.check_svg_source(path, report, inline=inline)
                    self.assertIsNone(root)
                    self.assertTrue(
                        any("not well-formed XML" in error for error in report.errors)
                    )

    def test_non_svg_xml_is_rejected_for_inline_and_standalone_svg(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_svg(directory, '<figure viewBox="0 0 10 10"/>')

            for inline in (False, True):
                with self.subTest(inline=inline):
                    report = validate_docs.Report()
                    root = validate_docs.check_svg_source(path, report, inline=inline)
                    self.assertIsNone(root)
                    self.assertTrue(
                        any("root element is not <svg>" in error for error in report.errors)
                    )

    def test_tc_figure_requires_the_canonical_fallback_theme(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_svg(
                directory,
                '<svg class="tc-figure" viewBox="0 0 10 10"><style/></svg>',
            )
            report = validate_docs.Report()
            validate_docs.check_svg_source(path, report)
            self.assertTrue(any("canonical SVG fallback theme" in error for error in report.errors))

            path = self.write_svg(
                directory,
                f'<svg class="tc-figure" viewBox="0 0 10 10">\n{SVG_THEME_BLOCK}\n</svg>',
            )
            report = validate_docs.Report()
            validate_docs.check_svg_source(path, report)
            self.assertEqual([], report.errors)


class CheckMermaidIncludeTests(unittest.TestCase):
    def test_included_diagram_is_checked_for_accessibility_and_colours(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "page.md").write_text("```mermaid\n{{#include figure.mmd}}\n```\n")
            figure = source / "figure.mmd"
            figure.write_text(
                "flowchart TB\naccTitle: Data flow\naccDescr: A sends data to B\nA --> B\n"
            )
            with patch.object(validate_docs, "SRC", source):
                report = validate_docs.Report()
                self.assertEqual(1, validate_docs.check_mermaid(report))
                self.assertEqual([], report.errors)
                self.assertEqual([], report.warnings)

                figure.write_text("flowchart TB\nA --> B\nstyle A fill:#ffffff\n")
                report = validate_docs.Report()
                validate_docs.check_mermaid(report)
                self.assertTrue(any("literal colour" in error for error in report.errors))
                self.assertTrue(any("accessible description" in warning for warning in report.warnings))

    def test_missing_diagram_include_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "page.md").write_text("```mermaid\n{{#include missing.mmd}}\n```\n")
            with patch.object(validate_docs, "SRC", source):
                report = validate_docs.Report()
                validate_docs.check_mermaid(report)
                self.assertTrue(any("include does not exist" in error for error in report.errors))


if __name__ == "__main__":
    unittest.main()
