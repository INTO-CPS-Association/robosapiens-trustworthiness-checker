import tempfile
import unittest
from pathlib import Path

import validate_docs


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


if __name__ == "__main__":
    unittest.main()
