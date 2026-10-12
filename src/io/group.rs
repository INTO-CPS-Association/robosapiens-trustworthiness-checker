//! Finite replay of a group of composed inputs by an explicit step schedule.
//!
//! A group owns several finite *members*, each an ordered sequence of whole
//! ticks, and a schedule of *steps*. Each step names the members that
//! contribute to one logical tick of the group and consumes exactly the next
//! tick of each. Order within a member is structural; the schedule alone
//! places one member's ticks among another's, so a clock sample sits exactly
//! where the schedule says, and a record and a clock sample are simultaneous
//! only when one step names both. Timestamps never merge or reorder ticks.
//!
//! The schedule is checked as a whole before the group yields anything:
//!
//! - every named member exists, and no step names one twice or none at all;
//! - every named member has a next tick, and no member has ticks left over;
//! - no two contributions to one step write the same slot;
//! - a step's optional `time` equals "now" after the step.
//!
//! "Now" is a zero-order hold: the latest phase-record or clock-sample
//! instant consumed so far. It never regresses, so a record admitted earlier
//! than the current "now" is accepted with its own time and leaves "now"
//! where it was. Knowledge and incomplete records do not advance it. The
//! `time` field only validates a schedule; it is never a source of time.
//!
//! Each tick keeps the origins of its contributions, which the group can
//! write as provenance: one JSON line per logical tick, in the order ticks
//! are yielded.

use std::collections::BTreeMap;
use std::io::Write;

use ecow::EcoString;
use serde::Serialize;

use crate::io::config::{ReplayStepConfig, ReplaySteps};

/// The name a group gives one of its members.
pub type MemberId = EcoString;

/// One step of a schedule: the members whose next tick forms one logical
/// tick, and optionally the "now" that tick must reach.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplayStep {
    pub members: Vec<MemberId>,
    pub time: Option<f64>,
    /// Where the step was written, for error messages.
    pub at: EcoString,
}

impl ReplayStep {
    fn from_config(config: &ReplayStepConfig, at: EcoString) -> Self {
        Self {
            members: config.step.iter().map(|member| member.into()).collect(),
            time: config.time.as_ref().map(|time| {
                time.as_f64()
                    .expect("a JSON number without arbitrary precision is an f64")
            }),
            at,
        }
    }
}

/// Why a schedule cannot be replayed.
#[derive(Debug, thiserror::Error)]
pub enum GroupError {
    #[error("step file `{path}` could not be read: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("{at}: {message}")]
    Malformed { at: EcoString, message: String },
    #[error(
        "step file `{path}` ends inside line {line}: a captured schedule ends with a newline, \
         so its last step was cut short"
    )]
    PrematureEof { path: String, line: usize },
    #[error("{at}: the step names no member")]
    EmptyStep { at: EcoString },
    #[error("{at}: `{member}` is not a member of this group; its members are {members}")]
    UnknownMember {
        at: EcoString,
        member: MemberId,
        members: String,
    },
    #[error("{at}: the step names member `{member}` twice; a step takes one tick of each member")]
    DuplicateMember { at: EcoString, member: MemberId },
    #[error("{at}: member `{member}` has no tick left for this step")]
    MissingMemberTick { at: EcoString, member: MemberId },
    #[error("{at}: members `{first}` and `{second}` both write `{slot}` in one step")]
    ConflictingWrites {
        at: EcoString,
        slot: EcoString,
        first: MemberId,
        second: MemberId,
    },
    #[error("{at}: the step declares time {declared}, but \"now\" after it is {now}")]
    TimeMismatch {
        at: EcoString,
        declared: f64,
        now: String,
    },
    #[error("member `{member}` has {remaining} tick(s) that no step consumes")]
    UnconsumedInput { member: MemberId, remaining: usize },
}

/// Read a schedule: a JSON Lines file of `{"step": [...], "time": t}`, or
/// steps written inline in the configuration.
pub fn read_steps(steps: &ReplaySteps) -> Result<Vec<ReplayStep>, GroupError> {
    match steps {
        ReplaySteps::Inline(steps) => Ok(steps
            .iter()
            .enumerate()
            .map(|(index, step)| {
                ReplayStep::from_config(step, format!("inline step {}", index + 1).into())
            })
            .collect()),
        ReplaySteps::File(path) => {
            let text = std::fs::read_to_string(path).map_err(|source| GroupError::Read {
                path: path.clone(),
                source,
            })?;
            if !text.is_empty() && !text.ends_with('\n') {
                return Err(GroupError::PrematureEof {
                    path: path.clone(),
                    line: text.lines().count(),
                });
            }
            let mut steps = Vec::new();
            for (index, line) in text.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                let at: EcoString = format!("step file `{path}` line {}", index + 1).into();
                let step: ReplayStepConfig =
                    serde_json::from_str(line).map_err(|error| GroupError::Malformed {
                        at: at.clone(),
                        message: error.to_string(),
                    })?;
                steps.push(ReplayStep::from_config(&step, at));
            }
            Ok(steps)
        }
    }
}

/// What kind of input a contribution came from. The kind decides whether its
/// instant advances "now".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OriginKind {
    Phase,
    Knowledge,
    Incomplete,
    Clock,
}

impl OriginKind {
    /// Phase records and clock samples are what the monitor treats as the
    /// current time; Knowledge and incomplete records are not.
    pub fn advances_now(self) -> bool {
        matches!(self, Self::Phase | Self::Clock)
    }
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
    pub member: MemberId,
    pub kind: OriginKind,
    #[serde(flatten)]
    pub locator: Locator,
    pub time: f64,
}

/// One whole tick of a member: the slots it writes and where it came from.
#[derive(Clone, Debug, PartialEq)]
pub struct MemberTick<T> {
    pub writes: Vec<(EcoString, T)>,
    pub origin: Origin,
}

/// One logical tick of the group.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupTick<T> {
    pub writes: BTreeMap<EcoString, T>,
    pub origins: Vec<Origin>,
}

/// Materialise a schedule over finite members, checking it as a whole.
pub fn schedule<T>(
    members: BTreeMap<MemberId, Vec<MemberTick<T>>>,
    steps: &[ReplayStep],
) -> Result<Vec<GroupTick<T>>, GroupError> {
    let known = members
        .keys()
        .map(|member| format!("`{member}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut queues: BTreeMap<MemberId, std::vec::IntoIter<MemberTick<T>>> = members
        .into_iter()
        .map(|(member, ticks)| (member, ticks.into_iter()))
        .collect();

    let mut now: Option<f64> = None;
    let mut ticks = Vec::with_capacity(steps.len());
    for step in steps {
        if step.members.is_empty() {
            return Err(GroupError::EmptyStep {
                at: step.at.clone(),
            });
        }
        let mut writes = BTreeMap::<EcoString, T>::new();
        let mut writers = BTreeMap::<EcoString, MemberId>::new();
        let mut origins = Vec::with_capacity(step.members.len());
        for (index, member) in step.members.iter().enumerate() {
            if step.members[..index].contains(member) {
                return Err(GroupError::DuplicateMember {
                    at: step.at.clone(),
                    member: member.clone(),
                });
            }
            let Some(queue) = queues.get_mut(member) else {
                return Err(GroupError::UnknownMember {
                    at: step.at.clone(),
                    member: member.clone(),
                    members: known.clone(),
                });
            };
            let Some(tick) = queue.next() else {
                return Err(GroupError::MissingMemberTick {
                    at: step.at.clone(),
                    member: member.clone(),
                });
            };
            for (slot, value) in tick.writes {
                if let Some(first) = writers.insert(slot.clone(), member.clone()) {
                    return Err(GroupError::ConflictingWrites {
                        at: step.at.clone(),
                        slot,
                        first,
                        second: member.clone(),
                    });
                }
                writes.insert(slot, value);
            }
            if tick.origin.kind.advances_now() {
                now = Some(now.map_or(tick.origin.time, |now| now.max(tick.origin.time)));
            }
            origins.push(tick.origin);
        }
        if let Some(declared) = step.time
            && now != Some(declared)
        {
            return Err(GroupError::TimeMismatch {
                at: step.at.clone(),
                declared,
                now: now.map_or_else(|| "not yet set".to_owned(), |now| now.to_string()),
            });
        }
        ticks.push(GroupTick { writes, origins });
    }
    for (member, queue) in queues {
        let remaining = queue.len();
        if remaining != 0 {
            return Err(GroupError::UnconsumedInput { member, remaining });
        }
    }
    Ok(ticks)
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

    fn tick(member: &str, slot: &str, kind: OriginKind, time: f64) -> MemberTick<f64> {
        MemberTick {
            writes: vec![(slot.into(), time)],
            origin: origin(member, kind, time),
        }
    }

    /// A journal of records and a clock of samples, by (kind, time).
    fn members(
        journal: &[(OriginKind, f64)],
        clock: &[f64],
    ) -> BTreeMap<MemberId, Vec<MemberTick<f64>>> {
        BTreeMap::from([
            (
                "journal".into(),
                journal
                    .iter()
                    .map(|&(kind, time)| tick("journal", "record", kind, time))
                    .collect(),
            ),
            (
                "clock".into(),
                clock
                    .iter()
                    .map(|&time| tick("clock", "clock", OriginKind::Clock, time))
                    .collect(),
            ),
        ])
    }

    fn steps(steps: &[(&[&str], Option<f64>)]) -> Vec<ReplayStep> {
        steps
            .iter()
            .enumerate()
            .map(|(index, (members, time))| ReplayStep {
                members: members.iter().map(|member| (*member).into()).collect(),
                time: *time,
                at: format!("step {}", index + 1).into(),
            })
            .collect()
    }

    use OriginKind::{Clock, Incomplete, Knowledge, Phase};

    #[test]
    fn a_step_takes_one_whole_tick_of_each_named_member() {
        let ticks = schedule(
            members(&[(Phase, 1.0), (Phase, 3.0)], &[2.0, 4.0]),
            &steps(&[
                (&["journal"], None),
                (&["clock"], None),
                (&["journal", "clock"], None),
            ]),
        )
        .unwrap();
        assert_eq!(ticks.len(), 3);
        assert_eq!(ticks[0].writes, BTreeMap::from([("record".into(), 1.0)]));
        assert_eq!(ticks[1].writes, BTreeMap::from([("clock".into(), 2.0)]));
        assert_eq!(
            ticks[2].writes,
            BTreeMap::from([("record".into(), 3.0), ("clock".into(), 4.0)])
        );
        assert_eq!(
            ticks[2].origins,
            vec![origin("journal", Phase, 3.0), origin("clock", Clock, 4.0)]
        );
    }

    #[test]
    fn now_is_held_and_only_phase_records_and_clocks_advance_it() {
        let journal = [
            (Phase, 5.0),
            (Knowledge, 9.0),
            (Incomplete, 9.5),
            (Phase, 3.0),
        ];
        let ticks = schedule(
            members(&journal, &[6.0]),
            &steps(&[
                (&["journal"], Some(5.0)),
                (&["journal"], Some(5.0)),
                (&["journal"], Some(5.0)),
                // A phase record admitted before "now" keeps its own time.
                (&["journal", "clock"], Some(6.0)),
            ]),
        )
        .unwrap();
        assert_eq!(ticks[3].writes["record"], 3.0);
        assert_eq!(ticks[3].origins[0].time, 3.0);

        let error = schedule(
            members(&[(Knowledge, 1.0)], &[]),
            &steps(&[(&["journal"], Some(1.0))]),
        )
        .unwrap_err();
        assert!(
            matches!(&error, GroupError::TimeMismatch { declared, .. } if *declared == 1.0),
            "{error}"
        );
        assert!(error.to_string().contains("not yet set"), "{error}");
    }

    #[test]
    fn a_backwards_record_is_admitted_and_a_regressing_now_is_refused() {
        let ticks = schedule(
            members(&[(Phase, 4.0), (Phase, 2.0)], &[]),
            &steps(&[(&["journal"], Some(4.0)), (&["journal"], None)]),
        )
        .unwrap();
        assert_eq!(ticks[1].writes["record"], 2.0);
        let error = schedule(
            members(&[(Phase, 4.0), (Phase, 2.0)], &[]),
            &steps(&[(&["journal"], Some(4.0)), (&["journal"], Some(2.0))]),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("declares time 2, but \"now\" after it is 4"),
            "{error}"
        );
    }

    #[test]
    fn a_schedule_is_refused_for_each_way_it_can_go_wrong() {
        let error = |journal: &[(OriginKind, f64)], clock: &[f64], plan: &[(&[&str], _)]| {
            schedule(members(journal, clock), &steps(plan)).unwrap_err()
        };
        assert!(matches!(
            error(&[(Phase, 1.0)], &[], &[(&["journal", "journal"], None)]),
            GroupError::DuplicateMember { member, .. } if member == "journal"
        ));
        let unknown = error(&[(Phase, 1.0)], &[], &[(&["sensor"], None)]);
        assert!(matches!(&unknown, GroupError::UnknownMember { member, .. } if member == "sensor"));
        assert!(
            unknown.to_string().contains("`clock`, `journal`"),
            "{unknown}"
        );
        assert!(matches!(
            error(&[], &[], &[(&[], None)]),
            GroupError::EmptyStep { .. }
        ));
        assert!(matches!(
            error(&[(Phase, 1.0)], &[], &[(&["journal"], None), (&["journal"], None)]),
            GroupError::MissingMemberTick { at, member } if at == "step 2" && member == "journal"
        ));
        assert!(matches!(
            error(&[(Phase, 1.0)], &[2.0, 3.0], &[(&["journal", "clock"], None)]),
            GroupError::UnconsumedInput { member, remaining: 1 } if member == "clock"
        ));

        let mut overlapping = members(&[(Phase, 1.0)], &[2.0]);
        overlapping.get_mut("clock").unwrap()[0].writes[0].0 = "record".into();
        let conflict = schedule(overlapping, &steps(&[(&["journal", "clock"], None)])).unwrap_err();
        assert!(
            matches!(&conflict, GroupError::ConflictingWrites { slot, first, second, .. }
                if slot == "record" && first == "journal" && second == "clock"),
            "{conflict}"
        );
    }

    fn step_file(text: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "tc-replay-steps-{}-{}.jsonl",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&path, text).unwrap();
        path.display().to_string()
    }

    #[test]
    fn a_step_file_is_json_lines_and_ends_with_a_newline() {
        let path = step_file(
            "{\"step\": [\"journal\"]}\n\n{\"step\": [\"journal\", \"clock\"], \"time\": 12.0}\n",
        );
        let steps = read_steps(&ReplaySteps::File(path.clone())).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            steps
                .iter()
                .map(|step| (step.members.clone(), step.time))
                .collect::<Vec<_>>(),
            vec![
                (vec!["journal".into()], None),
                (vec!["journal".into(), "clock".into()], Some(12.0)),
            ]
        );
        assert!(steps[1].at.ends_with("line 3"), "{}", steps[1].at);

        let cut = step_file("{\"step\": [\"journal\"]}\n{\"step\": [\"clo");
        let error = read_steps(&ReplaySteps::File(cut.clone())).unwrap_err();
        let _ = std::fs::remove_file(&cut);
        assert!(
            matches!(error, GroupError::PrematureEof { line: 2, .. }),
            "{error}"
        );

        let unknown_field = step_file("{\"step\": [\"journal\"], \"at\": 1.0}\n");
        let error = read_steps(&ReplaySteps::File(unknown_field.clone())).unwrap_err();
        let _ = std::fs::remove_file(&unknown_field);
        assert!(error.to_string().contains("line 1"), "{error}");
        assert!(error.to_string().contains("unknown field `at`"), "{error}");
    }

    #[test]
    fn provenance_is_one_json_line_per_tick() {
        let path = step_file("");
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
