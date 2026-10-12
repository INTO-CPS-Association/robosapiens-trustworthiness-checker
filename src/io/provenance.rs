//! Provenance of composed input ticks.
//!
//! A source whose ticks combine contributions, such as a journal record and
//! a clock sample, can write where each tick came from: one JSON line per
//! logical tick, `{"tick": k, "origins": [...]}`, in the order ticks are
//! yielded, so a finding printed on stdout row `k` can be traced back to
//! the records and samples that produced it.

use std::io::Write;

use ecow::EcoString;
use serde::Serialize;

/// What kind of input a contribution came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OriginKind {
    Phase,
    Knowledge,
    Clock,
}

/// Where a contribution can be found again: a record's cursor in its
/// journal, or the clock that took a sample.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Locator {
    Cursor(EcoString),
    Clock(EcoString),
}

/// The origin of one contribution to a logical tick.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Origin {
    /// The contributing input, such as `journal` or `clock`.
    pub member: EcoString,
    pub kind: OriginKind,
    #[serde(flatten)]
    pub locator: Locator,
    pub time: f64,
}

/// Writes one provenance line per logical tick: `{"tick": k, "origins":
/// [...]}`, flushed as each tick is yielded.
pub(crate) struct ProvenanceWriter {
    path: String,
    file: std::io::BufWriter<std::fs::File>,
    tick: u64,
}

impl ProvenanceWriter {
    /// Create (or truncate) the provenance file before any tick is yielded.
    pub(crate) fn create(path: &str) -> anyhow::Result<Self> {
        let file = std::fs::File::create(path).map_err(|error| {
            anyhow::anyhow!("provenance file `{path}` could not be created: {error}")
        })?;
        Ok(Self {
            path: path.to_owned(),
            file: std::io::BufWriter::new(file),
            tick: 0,
        })
    }

    pub(crate) fn write(&mut self, origins: &[Origin]) -> anyhow::Result<()> {
        #[derive(Serialize)]
        struct Line<'a> {
            tick: u64,
            origins: &'a [Origin],
        }
        let line = serde_json::to_string(&Line {
            tick: self.tick,
            origins,
        })?;
        writeln!(self.file, "{line}")
            .and_then(|()| self.file.flush())
            .map_err(|error| {
                anyhow::anyhow!(
                    "provenance file `{}` could not be written: {error}",
                    self.path
                )
            })?;
        self.tick += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::OriginKind::{Clock, Phase};
    use super::*;

    fn origin(member: &str, kind: OriginKind, time: f64) -> Origin {
        Origin {
            member: member.into(),
            kind,
            locator: match kind {
                OriginKind::Clock => Locator::Clock("host".into()),
                _ => Locator::Cursor(format!("{member}@{time}").into()),
            },
            time,
        }
    }

    #[test]
    fn provenance_is_one_json_line_per_tick() {
        let path = std::env::temp_dir()
            .join(format!(
                "tc-provenance-{}-{}.jsonl",
                std::process::id(),
                uuid::Uuid::new_v4()
            ))
            .display()
            .to_string();
        let mut writer = ProvenanceWriter::create(&path).unwrap();
        writer
            .write(&[origin("journal", Phase, 1.5), origin("clock", Clock, 2.0)])
            .unwrap();
        writer.write(&[origin("clock", Clock, 3.0)]).unwrap();
        drop(writer);
        let text = std::fs::read_to_string(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            text,
            "{\"tick\":0,\"origins\":[\
             {\"member\":\"journal\",\"kind\":\"phase\",\"cursor\":\"journal@1.5\",\"time\":1.5},\
             {\"member\":\"clock\",\"kind\":\"clock\",\"clock\":\"host\",\"time\":2.0}]}\n\
             {\"tick\":1,\"origins\":[\
             {\"member\":\"clock\",\"kind\":\"clock\",\"clock\":\"host\",\"time\":3.0}]}\n"
        );
    }
}
