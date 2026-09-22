//! Process-independent orchestration for the developer `tc-expand` command.

use std::io::{self, Write};

use crate::{
    cli::diagnostics::render_warnings,
    lang::dsrv::{
        TypeCheckMode, TypeCheckOptions,
        inspection::{CheckedSourceView, ExpandedView, render_checked, render_expanded},
        program::load_program_file,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckMode {
    Strict,
    Gradual,
}

#[derive(Debug, thiserror::Error)]
pub enum ExpandIoError {
    #[error("writing tc-expand output: {0}")]
    Stdout(#[source] io::Error),
    #[error("writing tc-expand diagnostics: {0}")]
    Stderr(#[source] io::Error),
}

impl ExpandIoError {
    pub fn kind(&self) -> io::ErrorKind {
        match self {
            Self::Stdout(error) | Self::Stderr(error) => error.kind(),
        }
    }
}

/// Load and inspect `model`, writing diagnostics only to `stderr`.
///
/// The complete report is built before the first stdout write.
pub fn run(
    model: &str,
    check_mode: Option<CheckMode>,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> Result<bool, ExpandIoError> {
    let loaded = match smol::block_on(load_program_file(model, Default::default())) {
        Ok(loaded) => loaded,
        Err(error) => {
            writeln!(stderr, "error: {error:#}").map_err(ExpandIoError::Stderr)?;
            stderr.flush().map_err(ExpandIoError::Stderr)?;
            return Ok(false);
        }
    };

    let report = match check_mode {
        None => render_expanded(ExpandedView {
            specification: &loaded.specification,
            modules: &loaded.modules,
        }),
        Some(mode) => {
            let options = TypeCheckOptions {
                mode: match mode {
                    CheckMode::Strict => TypeCheckMode::Strict,
                    CheckMode::Gradual => TypeCheckMode::Gradual,
                },
            };
            let (checked, warnings) = loaded.specification.check(options).into_parts();
            render_warnings(stderr, model, &loaded.root_source, &warnings)
                .map_err(ExpandIoError::Stderr)?;
            let checked = match checked {
                Ok(checked) => checked,
                Err(errors) => {
                    for error in errors {
                        writeln!(stderr, "error: {error:?}").map_err(ExpandIoError::Stderr)?;
                    }
                    stderr.flush().map_err(ExpandIoError::Stderr)?;
                    return Ok(false);
                }
            };
            render_checked(CheckedSourceView {
                specification: &checked,
                modules: &loaded.modules,
            })
        }
    };

    stdout
        .write_all(report.as_bytes())
        .map_err(ExpandIoError::Stdout)?;
    stdout.flush().map_err(ExpandIoError::Stdout)?;
    stderr.flush().map_err(ExpandIoError::Stderr)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        io::{Error, ErrorKind},
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn model(source: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "tc-expand-{}-{}.dsrv",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, source).unwrap();
        path
    }

    #[test]
    fn plain_strict_and_gradual_use_the_requested_view() {
        let path = model("out y: Int\ny = 1");
        for (mode, marker) in [
            (None, "expanded, unchecked"),
            (Some(CheckMode::Strict), "checked (strict)"),
            (Some(CheckMode::Gradual), "checked (gradual)"),
        ] {
            let (mut out, mut err) = (Vec::new(), Vec::new());
            assert!(run(path.to_str().unwrap(), mode, &mut out, &mut err).unwrap());
            assert!(String::from_utf8(out).unwrap().contains(marker));
            assert!(err.is_empty());
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn failed_check_keeps_stdout_empty_and_diagnostics_on_stderr() {
        let path = model("out y: Bool\ny = 1");
        let (mut out, mut err) = (Vec::new(), Vec::new());
        assert!(
            !run(
                path.to_str().unwrap(),
                Some(CheckMode::Strict),
                &mut out,
                &mut err
            )
            .unwrap()
        );
        assert!(out.is_empty());
        assert!(String::from_utf8(err).unwrap().contains("error:"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn successful_warnings_stay_on_stderr() {
        let path = model("use experimental::{casts}\nout y = 1 as Int");
        let (mut out, mut err) = (Vec::new(), Vec::new());
        assert!(
            run(
                path.to_str().unwrap(),
                Some(CheckMode::Gradual),
                &mut out,
                &mut err
            )
            .unwrap()
        );
        assert!(
            String::from_utf8(out)
                .unwrap()
                .contains("checked (gradual)")
        );
        assert!(
            String::from_utf8(err)
                .unwrap()
                .contains("warning[dsrv.redundant-cast]")
        );
        fs::remove_file(path).unwrap();
    }

    struct Fails {
        kind: ErrorKind,
        flush: bool,
    }

    impl Write for Fails {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            if self.flush {
                Ok(1)
            } else {
                Err(Error::from(self.kind))
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.flush {
                Err(Error::from(self.kind))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn stdout_write_broken_pipe_and_flush_failures_are_returned() {
        let path = model("out y: Int\ny = 1");
        for mut writer in [
            Fails {
                kind: ErrorKind::BrokenPipe,
                flush: false,
            },
            Fails {
                kind: ErrorKind::Other,
                flush: true,
            },
        ] {
            let error =
                run(path.to_str().unwrap(), None, &mut writer, &mut Vec::new()).unwrap_err();
            assert!(matches!(error, ExpandIoError::Stdout(_)));
            assert_eq!(error.kind(), writer.kind);
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn stderr_write_and_flush_failures_keep_the_diagnostic_channel() {
        for mut writer in [
            Fails {
                kind: ErrorKind::BrokenPipe,
                flush: false,
            },
            Fails {
                kind: ErrorKind::Other,
                flush: true,
            },
        ] {
            let error = run(
                "/a/model/that/does/not/exist.dsrv",
                None,
                &mut Vec::new(),
                &mut writer,
            )
            .unwrap_err();
            assert!(matches!(error, ExpandIoError::Stderr(_)));
            assert_eq!(error.kind(), writer.kind);
        }
    }

    struct ZeroProgress;

    impl Write for ZeroProgress {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Ok(0)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct PartialThenFail {
        wrote: bool,
    }

    impl Write for PartialThenFail {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            if std::mem::replace(&mut self.wrote, true) {
                Err(Error::from(ErrorKind::Other))
            } else {
                Ok(data.len().min(3))
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct ShortWriter(Vec<u8>);

    impl Write for ShortWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            let written = data.len().min(3);
            self.0.extend_from_slice(&data[..written]);
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn stdout_handles_zero_partial_and_successful_short_writes() {
        let path = model("out y: Int\ny = 1");
        let mut zero = ZeroProgress;
        let error = run(path.to_str().unwrap(), None, &mut zero, &mut Vec::new()).unwrap_err();
        assert!(matches!(error, ExpandIoError::Stdout(_)));
        assert_eq!(error.kind(), ErrorKind::WriteZero);

        let mut partial = PartialThenFail { wrote: false };
        assert!(matches!(
            run(path.to_str().unwrap(), None, &mut partial, &mut Vec::new()),
            Err(ExpandIoError::Stdout(_))
        ));

        let mut short = ShortWriter::default();
        assert!(run(path.to_str().unwrap(), None, &mut short, &mut Vec::new()).unwrap());
        assert!(
            String::from_utf8(short.0)
                .unwrap()
                .contains("expanded, unchecked")
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn warnings_survive_a_failed_check_without_stdout_fallback() {
        let path = model("out y: Str = \"warn:alpha\"\nout z: Bool = 1");
        let (mut out, mut err) = (Vec::new(), Vec::new());
        assert!(
            !run(
                path.to_str().unwrap(),
                Some(CheckMode::Strict),
                &mut out,
                &mut err
            )
            .unwrap()
        );
        let diagnostics = String::from_utf8(err).unwrap();
        assert!(out.is_empty());
        assert!(diagnostics.contains("warning[test-alpha]"));
        assert!(diagnostics.contains("error:"));
        fs::remove_file(path).unwrap();
    }
}
