//! Presenting semantic warnings on the command line.
//!
//! Warnings are dedicated diagnostics on standard error. They do not pass
//! through tracing, so `RUST_LOG` and `--log-file` neither hide nor redirect
//! them, and they never reach standard output, which carries monitor output.
//!
//! Each warning is shown against the text that was checked: the root file as
//! the loader read it, or the submitted text of a live replacement. A
//! position is one-based, in lines and Unicode scalar values, followed by
//! the span's byte range. A span that does not fit the text is shown as its
//! byte range alone, and a warning without a span is shown without a
//! position.

use std::io::{self, Write};

use anyhow::Context;

use crate::lang::dsrv::diagnostics::SemanticWarning;
use crate::lang::dsrv::span::Span;

/// The label a live replacement's warnings are shown under; it has no file.
pub const REPLACEMENT_LABEL: &str = "<reconfiguration>";

/// Write `warnings`, in order, as diagnostics against `source`, which is
/// named `label`.
pub fn render_warnings(
    out: &mut impl Write,
    label: &str,
    source: &str,
    warnings: &[SemanticWarning],
) -> io::Result<()> {
    for warning in warnings {
        writeln!(out, "warning[{}]: {}", warning.code(), warning.message())?;
        match warning.span() {
            None => writeln!(out, "  --> {label}")?,
            Some(span) => match position(source, span) {
                Some((line, column)) => writeln!(
                    out,
                    "  --> {label}:{line}:{column} (bytes {}..{})",
                    span.start, span.end
                )?,
                None => writeln!(out, "  --> {label} (bytes {}..{})", span.start, span.end)?,
            },
        }
    }
    out.flush()
}

/// Write `warnings` to standard error. Failing to write them is an error.
pub fn present_warnings(
    label: &str,
    source: &str,
    warnings: &[SemanticWarning],
) -> anyhow::Result<()> {
    render_warnings(&mut io::stderr().lock(), label, source, warnings)
        .context("semantic warnings could not be written to standard error")
}

/// The one-based line and Unicode-scalar column at which `span` starts, if
/// `span` lies within `source` on character boundaries.
fn position(source: &str, span: Span) -> Option<(usize, usize)> {
    let (start, end) = (span.start as usize, span.end as usize);
    if start > end || !source.is_char_boundary(start) || !source.is_char_boundary(end) {
        return None;
    }
    let before = &source[..start];
    let line_start = before.rfind('\n').map_or(0, |newline| newline + 1);
    Some((
        before.matches('\n').count() + 1,
        before[line_start..].chars().count() + 1,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::diagnostics::SemanticWarningKind;

    fn warning(kind: SemanticWarningKind, span: Option<Span>) -> SemanticWarning {
        SemanticWarning::new(kind, "fixture message", span)
    }

    fn rendered(source: &str, warnings: &[SemanticWarning]) -> String {
        let mut out = Vec::new();
        render_warnings(&mut out, "model.dsrv", source, warnings).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn a_position_counts_lines_and_unicode_scalars_from_one() {
        let source = "in x: Int\nout ÿé: Str = \"warn\"";
        let start = source.find("\"warn\"").unwrap();
        let span = Span::new(start as u32, (start + 6) as u32);
        assert_eq!(
            rendered(
                source,
                &[warning(SemanticWarningKind::TestAlpha, Some(span))]
            ),
            format!(
                "warning[test-alpha]: fixture message\n  --> model.dsrv:2:15 (bytes {start}..{})\n",
                start + 6
            )
        );
    }

    #[test]
    fn a_span_that_does_not_fit_the_text_shows_its_bytes_alone() {
        let source = "out é: Str";
        for span in [
            // Beyond the end.
            Span::new(4, 40),
            // Inside the two bytes of `é`.
            Span::new(5, 6),
            // Reversed.
            Span::new(6, 4),
        ] {
            assert_eq!(
                rendered(
                    source,
                    &[warning(SemanticWarningKind::TestBeta, Some(span))]
                ),
                format!(
                    "warning[test-beta]: fixture message\n  --> model.dsrv (bytes {}..{})\n",
                    span.start, span.end
                )
            );
        }
    }

    #[test]
    fn a_warning_without_a_span_is_shown_without_a_position() {
        assert_eq!(
            rendered(
                "out y: Str",
                &[warning(SemanticWarningKind::TestBeta, None)]
            ),
            "warning[test-beta]: fixture message\n  --> model.dsrv\n"
        );
    }

    #[test]
    fn warnings_are_rendered_in_the_order_given() {
        let source = "abc";
        let text = rendered(
            source,
            &[
                warning(SemanticWarningKind::TestBeta, Some(Span::new(2, 3))),
                warning(SemanticWarningKind::TestAlpha, None),
                warning(SemanticWarningKind::TestAlpha, Some(Span::new(0, 1))),
            ],
        );
        assert_eq!(
            text,
            "warning[test-beta]: fixture message\n  --> model.dsrv:1:3 (bytes 2..3)\n\
             warning[test-alpha]: fixture message\n  --> model.dsrv\n\
             warning[test-alpha]: fixture message\n  --> model.dsrv:1:1 (bytes 0..1)\n"
        );
    }

    #[test]
    fn no_warnings_render_nothing() {
        assert_eq!(rendered("out y: Str", &[]), "");
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("closed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("closed"))
        }
    }

    #[test]
    fn a_failed_write_is_returned() {
        let error = render_warnings(
            &mut FailingWriter,
            "model.dsrv",
            "",
            &[warning(SemanticWarningKind::TestAlpha, None)],
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "closed");
    }
}
