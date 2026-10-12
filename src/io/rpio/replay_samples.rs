//! Bounded, prefix-covered clock schedules for classic capture replay.
//! Files are validated in one pass, rewound, then consumed one line at a time.
//! Their second-pass digest must match, so a changed schedule cannot finish as
//! a valid experiment. No vector of file samples or projected ticks is built.

use std::io::{BufRead, BufReader, Read, Seek};

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};

use crate::io::config::{RpioReplaySample, RpioReplaySamples};

const MAX_LINE_BYTES: u64 = 1_048_576;

enum Reader {
    Inline(std::iter::Enumerate<std::vec::IntoIter<RpioReplaySample>>),
    File {
        path: String,
        reader: BufReader<std::fs::File>,
        line: usize,
        digest: Sha256,
        expected_digest: Option<[u8; 32]>,
    },
}

pub(crate) struct Samples {
    reader: Reader,
}

impl Samples {
    pub(crate) fn open(schedule: RpioReplaySamples, records: usize) -> Result<Self> {
        let mut source = match schedule {
            RpioReplaySamples::Inline(samples) => {
                Self::validate(
                    samples
                        .iter()
                        .enumerate()
                        .map(|(i, s)| Ok((s.clone(), format!("clock sample {}", i + 1)))),
                    records,
                )?;
                Self {
                    reader: Reader::Inline(samples.into_iter().enumerate()),
                }
            }
            RpioReplaySamples::File(path) => Self {
                reader: Reader::File {
                    reader: BufReader::new(std::fs::File::open(&path).with_context(|| {
                        format!("clock sample file `{path}` could not be opened")
                    })?),
                    path,
                    line: 0,
                    digest: Sha256::new(),
                    expected_digest: None,
                },
            },
        };
        if matches!(source.reader, Reader::File { .. }) {
            Self::validate(std::iter::from_fn(|| source.next_at().transpose()), records)?;
            let Reader::File {
                reader,
                line,
                digest,
                expected_digest,
                ..
            } = &mut source.reader
            else {
                unreachable!()
            };
            *expected_digest = Some(digest.clone().finalize().into());
            reader
                .rewind()
                .context("clock sample file could not be rewound")?;
            *line = 0;
            *digest = Sha256::new();
        }
        Ok(source)
    }

    fn validate(
        samples: impl Iterator<Item = Result<(RpioReplaySample, String)>>,
        records: usize,
    ) -> Result<()> {
        let mut previous = None;
        for item in samples {
            let (sample, at) = item?;
            ensure!(
                sample.time.as_f64().is_some_and(f64::is_finite),
                "{at}: clock time must be finite"
            );
            ensure!(
                sample.through <= records,
                "{at} covers records through {}, outside the replay interval after 0 through {records}",
                sample.through
            );
            ensure!(
                previous.is_none_or(|p| sample.through >= p),
                "{at} covers records through {}, before the previous sample's {}",
                sample.through,
                previous.unwrap_or(0)
            );
            ensure!(
                !sample.with_record || (sample.through > 0 && previous != Some(sample.through)),
                "{at}: with_record must be the first sample at a nonempty record prefix"
            );
            previous = Some(sample.through);
        }
        Ok(())
    }

    fn next_at(&mut self) -> Result<Option<(RpioReplaySample, String)>> {
        match &mut self.reader {
            Reader::Inline(samples) => Ok(samples
                .next()
                .map(|(i, s)| (s, format!("clock sample {}", i + 1)))),
            Reader::File {
                path,
                reader,
                line,
                digest,
                expected_digest,
            } => loop {
                let mut text = String::new();
                let bytes = (&mut *reader)
                    .take(MAX_LINE_BYTES + 1)
                    .read_line(&mut text)
                    .with_context(|| {
                        format!(
                            "clock sample file `{path}` line {} could not be read",
                            *line + 1
                        )
                    })?;
                if bytes == 0 {
                    if let Some(expected) = expected_digest {
                        let actual: [u8; 32] = digest.clone().finalize().into();
                        ensure!(
                            actual == *expected,
                            "clock sample file `{path}` changed after validation"
                        );
                    }
                    return Ok(None);
                }
                *line += 1;
                let at = format!("clock sample file `{path}` line {line}");
                ensure!(
                    bytes as u64 <= MAX_LINE_BYTES,
                    "{at}: clock sample line exceeds {MAX_LINE_BYTES} bytes"
                );
                digest.update(text.as_bytes());
                if text.trim().is_empty() {
                    continue;
                }
                let sample = serde_json::from_str(&text).with_context(|| at.clone())?;
                return Ok(Some((sample, at)));
            },
        }
    }

    pub(crate) fn next(&mut self) -> Result<Option<RpioReplaySample>> {
        Ok(self.next_at()?.map(|(sample, _)| sample))
    }
}
