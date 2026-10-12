//! RPIO journal records and clock samples as one typed observation input.
//!
//! The composed source delivers one input variable whose every tick is the
//! DSRV value `rpio::journal::timing::Observation<K, E, M, D>`:
//!
//! ```text
//! Struct<record: Option<TimedValue<Record<K, E, M>, D>>,
//!        clock: Option<ClockSample<D>>>
//! ```
//!
//! A record arrives with its journal admission time as `record.time`, an
//! `std::time::Instant<D>`; the record itself is unchanged, so its
//! `arrival.journal_time` is the same instant as a `Float`. A clock sample
//! carries its clock, its instant and how it was taken. A tick with no
//! record has `record = None`, never a repetition of the last record.
//!
//! The source is bound to one domain `D`, named by the specification's
//! declaration of the input or by configuration, and to the label of the
//! clock that stamps journal times. A live journal entry carrying another
//! label is an input error at that entry. Finite files carry no label, so
//! configuration binds them.
//!
//! Finite replay is a [group](crate::io::group) of two members, `journal`
//! and `clock`, scheduled by explicit steps; live input delivers each record
//! and each covered Redis-server clock sample on a tick of its own.

use std::collections::BTreeMap;

use anyhow::Context;
use ecow::EcoString;

use crate::core::time::{ClockId, ClockSample, SampleOrigin, SecondsDomain};
use crate::core::{InputBatch, InputStream, StreamType, UnionValue, Value, ValueMap, VarName};
use crate::io::config::ReplaySteps;
use crate::io::group::{
    self, GroupTick, Locator, MemberId, MemberTick, Origin, OriginKind, ProvenanceWriter,
    ReplayStep,
};

use super::JournalArrival;

/// The member of a finite group that replays the journal files.
pub(crate) const JOURNAL: &str = "journal";
/// The member of a finite group that replays the configured clock samples.
pub(crate) const CLOCK: &str = "clock";

const RECORD_SLOT: &str = "record";
const CLOCK_SLOT: &str = "clock";

/// What binds a composed source's instants to a time domain.
#[derive(Clone, Debug)]
pub(crate) struct ObservationBinding {
    pub variable: VarName,
    /// The canonical spelling of the domain marker `D`, once known.
    pub domain: Option<EcoString>,
    pub journal_clock: EcoString,
    pub declared: Option<StreamType>,
}

impl ObservationBinding {
    /// The bound domain. A source opened without one has nothing to brand
    /// its instants with, so it is refused before it reads anything.
    pub(crate) fn domain(&self) -> anyhow::Result<SecondsDomain> {
        let marker = self.domain.as_ref().with_context(|| {
            format!(
                "RPIO observation-clock input `{}` has no time domain: declare the input's type \
                 as an Observation over std::time::Instant<D>, or name D as `domain` in its \
                 source configuration",
                self.variable
            )
        })?;
        Ok(SecondsDomain::new(
            marker.clone(),
            self.journal_clock.clone(),
        ))
    }

    /// Bind the domain the specification declares for this source's
    /// variable. A configured domain must agree with the declaration; a
    /// declaration that names no instant type leaves the binding as it is.
    pub(crate) fn bind_declared(&mut self, declared: &StreamType) -> anyhow::Result<()> {
        self.declared = Some(declared.clone());
        let Some(marker) = declared_domain(declared).with_context(|| {
            format!(
                "the declared type of RPIO observation input `{}` cannot carry this source's \
                 instants",
                self.variable
            )
        })?
        else {
            return Ok(());
        };
        match &self.domain {
            Some(configured) if *configured != marker => anyhow::bail!(
                "RPIO observation input `{}` is declared over time domain `{marker}`, but its \
                 source configuration binds domain `{configured}`",
                self.variable
            ),
            _ => self.domain = Some(marker),
        }
        Ok(())
    }
}

/// The domain marker an observation type declares through the instants of
/// its record and clock sample, if it declares one.
fn declared_domain(declared: &StreamType) -> anyhow::Result<Option<EcoString>> {
    let instant_of = |field: &str| -> Option<&StreamType> {
        let StreamType::Union(option) = struct_field(declared, field)? else {
            return None;
        };
        let some = option
            .alternatives()
            .iter()
            .find(|alternative| alternative.tag() == "Some")?;
        let crate::core::UnionPayload::Of(payload) = some.payload() else {
            return None;
        };
        struct_field(payload, "time")
    };
    let mut found: Option<EcoString> = None;
    for field in [RECORD_SLOT, CLOCK_SLOT] {
        let Some(instant) = instant_of(field) else {
            continue;
        };
        let Some(marker) = SecondsDomain::marker_of_instant(instant)? else {
            continue;
        };
        match &found {
            Some(previous) if *previous != marker => anyhow::bail!(
                "its record instants are in domain `{previous}` and its clock instants in `{marker}`"
            ),
            _ => found = Some(marker),
        }
    }
    Ok(found)
}

fn struct_field<'a>(ty: &'a StreamType, name: &str) -> Option<&'a StreamType> {
    let StreamType::Struct(fields, _) = ty else {
        return None;
    };
    fields
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, ty)| ty)
}

fn option(value: Option<Value>) -> Value {
    UnionValue::new(if value.is_some() { "Some" } else { "None" }, value).into()
}

/// `TimedValue<Record, D>`: the record and its admission instant.
fn timed_record(domain: &SecondsDomain, time: f64, record: Value) -> Value {
    Value::Map(ValueMap::from([
        ("time".into(), domain.instant(time)),
        ("value".into(), record),
    ]))
}

/// One tick of the observation input, from what the tick's contributions
/// wrote.
fn observation(mut writes: BTreeMap<EcoString, Value>) -> Value {
    Value::Map(ValueMap::from([
        (RECORD_SLOT.into(), option(writes.remove(RECORD_SLOT))),
        (CLOCK_SLOT.into(), option(writes.remove(CLOCK_SLOT))),
    ]))
}

/// A record's kind and admission time, as its origin reports them.
fn record_origin(record: &Value) -> anyhow::Result<(OriginKind, f64)> {
    let kind = match record {
        Value::Union(record) if record.tag() == "Phase" => OriginKind::Phase,
        Value::Union(record) if record.tag() == "Knowledge" => OriginKind::Knowledge,
        Value::Union(record) if record.tag() == "Incomplete" => OriginKind::Incomplete,
        other => anyhow::bail!("an RPIO observation record is not a Record: {other:?}"),
    };
    let time = JournalArrival::<SecondsDomain>::try_from(record)?
        .0
        .context("an RPIO observation record has no arrival time")?;
    Ok((kind, time))
}

/// A journal record as a contribution to one tick.
fn record_tick(
    domain: &SecondsDomain,
    member: &str,
    cursor: EcoString,
    record: Value,
) -> anyhow::Result<MemberTick<Value>> {
    let (kind, time) = record_origin(&record)?;
    Ok(MemberTick {
        writes: vec![(RECORD_SLOT.into(), timed_record(domain, time, record))],
        origin: Origin {
            member: member.into(),
            kind,
            locator: Locator::Cursor(cursor),
            time,
        },
    })
}

/// A clock sample as a contribution to one tick.
fn clock_tick(
    domain: &SecondsDomain,
    member: &str,
    sample: &ClockSample<SecondsDomain>,
) -> MemberTick<Value> {
    MemberTick {
        writes: vec![(CLOCK_SLOT.into(), sample.to_value(domain))],
        origin: Origin {
            member: member.into(),
            kind: OriginKind::Clock,
            locator: Locator::Clock(sample.clock.as_str().into()),
            time: sample.time,
        },
    }
}

/// A finite composed source: journal files and replayed clock samples.
#[derive(Clone, Debug)]
pub(crate) struct FiniteObservationClock {
    pub binding: ObservationBinding,
    pub files: Vec<String>,
    pub capture_checks: Vec<crate::io::config::RpioCaptureCheck>,
    pub clock_values: Vec<f64>,
    pub clock_samples: Option<crate::io::config::RpioReplaySamples>,
    pub steps: Option<ReplaySteps>,
    pub provenance: Option<String>,
    /// How the journal files are read: capabilities removed and unrecorded versions.
    pub reading: super::JournalReading,
}

impl FiniteObservationClock {
    fn records(&self) -> anyhow::Result<Vec<super::observation_file::FileRecord>> {
        for check in &self.capture_checks {
            anyhow::ensure!(
                check.journal_clock.as_str() == self.binding.journal_clock.as_str(),
                "RPIO capture clock `{}` differs from configured journal_clock `{}`",
                check.journal_clock,
                self.binding.journal_clock
            );
        }
        super::observation_file::read_records_checked(
            &self.files,
            &self.reading,
            &self.capture_checks,
        )
    }

    /// Read both members, check the whole schedule, and return the ticks
    /// with their origins. Nothing is yielded from a schedule that fails.
    pub(crate) fn materialise(&self) -> anyhow::Result<Vec<GroupTick<Value>>> {
        let domain = self.binding.domain()?;
        let records = self.records()?;
        let journal = records
            .into_iter()
            .map(|record| {
                let cursor = record.cursor.clone();
                record_tick(&domain, JOURNAL, record.cursor, record.value)
                    .with_context(|| format!("RPIO observation record at `{cursor}`"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let clock = self
            .clock_values
            .iter()
            .map(|&time| {
                clock_tick(
                    &domain,
                    CLOCK,
                    &ClockSample {
                        clock: ClockId::new(self.binding.journal_clock.clone()),
                        time,
                        origin: SampleOrigin::Replay,
                    },
                )
            })
            .collect::<Vec<_>>();
        let steps = match &self.steps {
            Some(steps) => group::read_steps(steps)?,
            // Without a schedule the journal plays first and the samples
            // follow it, each on a tick of its own.
            None => std::iter::repeat_n(JOURNAL, journal.len())
                .chain(std::iter::repeat_n(CLOCK, clock.len()))
                .enumerate()
                .map(|(index, member)| ReplayStep {
                    members: vec![MemberId::from(member)],
                    time: None,
                    at: format!("default step {}", index + 1).into(),
                })
                .collect(),
        };
        let members = BTreeMap::from([(JOURNAL.into(), journal), (CLOCK.into(), clock)]);
        Ok(group::schedule(members, &steps)?)
    }

    pub(crate) fn open(mut self) -> anyhow::Result<InputStream<Value>> {
        if let Some(samples) = self.clock_samples.take() {
            return self.open_sampled(samples);
        }
        let ticks = self.materialise()?;
        let mut provenance = self
            .provenance
            .as_deref()
            .map(ProvenanceWriter::create)
            .transpose()?;
        let variable = self.binding.variable;
        Ok(Box::pin(async_stream::try_stream! {
            for tick in ticks {
                if let Some(provenance) = &mut provenance {
                    provenance.write(&tick.origins)?;
                }
                yield InputBatch::update(variable.clone(), super::admission::observation(observation(tick.writes), &self.binding.declared)?);
            }
        }))
    }

    fn open_sampled(
        self,
        schedule: crate::io::config::RpioReplaySamples,
    ) -> anyhow::Result<InputStream<Value>> {
        anyhow::ensure!(
            self.clock_values.is_empty() && self.steps.is_none(),
            "clock_samples cannot be mixed with clock_values or steps"
        );
        let domain = self.binding.domain()?;
        let records = self.records()?;
        // Validate the whole schedule before opening provenance or yielding.
        // File samples remain in the rewound reader, rather than a Vec.
        let mut samples = super::replay_samples::Samples::open(schedule, records.len())?;
        let clock = ClockId::new(self.binding.journal_clock.clone());
        let variable = self.binding.variable;
        let declared = self.binding.declared;
        let mut provenance = self
            .provenance
            .as_deref()
            .map(ProvenanceWriter::create)
            .transpose()?;
        Ok(Box::pin(async_stream::try_stream! {
            let mut pending = samples.next()?;
            let sample_tick = |sample: &crate::io::config::RpioReplaySample| {
                clock_tick(&domain, CLOCK, &ClockSample {
                    clock: clock.clone(),
                    time: sample.time.as_f64().expect("validated finite clock sample"),
                    origin: SampleOrigin::Replay,
                })
            };
            for (index, record) in records.into_iter().enumerate() {
                let position = index + 1;
                while pending.as_ref().is_some_and(|s| s.through < position) {
                    let tick = sample_tick(pending.as_ref().expect("checked sample"));
                    let origins = [tick.origin];
                    let value = super::admission::observation(observation(tick.writes.into_iter().collect()), &declared)?;
                    if let Some(p) = &mut provenance { p.write(&origins)?; }
                    yield InputBatch::update(variable.clone(), value);
                    pending = samples.next()?;
                }
                let mut tick = record_tick(&domain, JOURNAL, record.cursor, record.value)?;
                let mut origins = vec![tick.origin];
                if pending.as_ref().is_some_and(|s| s.through == position && s.with_record) {
                    let sample = sample_tick(pending.as_ref().expect("checked sample"));
                    tick.writes.extend(sample.writes);
                    origins.push(sample.origin);
                    pending = samples.next()?;
                }
                let value = super::admission::observation(observation(tick.writes.into_iter().collect()), &declared)?;
                if let Some(p) = &mut provenance { p.write(&origins)?; }
                yield InputBatch::update(variable.clone(), value);
            }
            while let Some(sample) = pending {
                let tick = sample_tick(&sample);
                let value = super::admission::observation(observation(tick.writes.into_iter().collect()), &declared)?;
                if let Some(p) = &mut provenance { p.write(&[tick.origin])?; }
                yield InputBatch::update(variable.clone(), value);
                pending = samples.next()?;
            }
        }))
    }
}

/// A live two-stream journal with fair, evidence-covered Redis clock samples.
#[derive(Clone, Debug)]
pub(crate) struct LiveObservationClock {
    pub binding: ObservationBinding,
    pub host: String,
    pub port: Option<u16>,
    pub database: u32,
    pub phase_stream: String,
    pub knowledge_stream: String,
    pub period: Option<std::time::Duration>,
    pub provenance: Option<String>,
}

#[cfg(feature = "redis")]
impl LiveObservationClock {
    pub(crate) async fn open(
        self,
        probe: crate::io::source_diagnostics::SourceProbe,
    ) -> anyhow::Result<(
        InputStream<Value>,
        std::rc::Rc<super::observation_redis::StopSignal>,
    )> {
        use super::observation_redis::{LiveTick, REDIS_SERVER_CLOCK};
        use crate::io::source_diagnostics::SourcePhase;
        use futures::StreamExt;
        anyhow::ensure!(
            self.period.is_none() || self.binding.journal_clock == REDIS_SERVER_CLOCK,
            "RPIO observation input `{}` samples Redis server TIME (`{REDIS_SERVER_CLOCK}`), but its journal_clock is `{}`",
            self.binding.variable,
            self.binding.journal_clock
        );
        let domain = self.binding.domain()?;
        let mut labels = super::time::LabelCheck::new(self.binding.journal_clock.clone());
        let mut provenance = self
            .provenance
            .as_deref()
            .map(ProvenanceWriter::create)
            .transpose()?;
        let (mut ticks, stop) = super::observation_redis::open_ticks(
            self.host,
            self.port,
            self.database,
            self.phase_stream,
            self.knowledge_stream,
            self.period,
            probe.clone(),
        )
        .await?;
        let variable = self.binding.variable;
        let declared = self.binding.declared;
        Ok((
            Box::pin(async_stream::try_stream! {
                while let Some(tick) = ticks.next().await {
                    let project = probe.stamp();
                    let tick = match tick? {
                        LiveTick::Record(record) => {
                            labels.check(&record.journal_clock).map_err(|error| anyhow::anyhow!("RPIO observation entry `{}`: {error}",record.cursor))?;
                            let cursor=record.cursor.clone();
                            record_tick(&domain,JOURNAL,record.cursor,record.value)
                                .map_err(|error| anyhow::anyhow!("RPIO observation entry `{cursor}`: {error:#}"))?
                        }
                        LiveTick::Clock(sample) => clock_tick(&domain,CLOCK,&sample),
                    };
                    let projected = observation(tick.writes.into_iter().collect());
                    probe.phase(project, SourcePhase::InputProjection);
                    let conform = probe.stamp();
                    let admitted = super::admission::observation(projected, &declared);
                    probe.phase(conform, SourcePhase::InputConformance);
                    let value = admitted?;
                    if let Some(provenance)=&mut provenance {
                        let written=probe.stamp();
                        let result=provenance.write(std::slice::from_ref(&tick.origin));
                        probe.phase(written,SourcePhase::InputProvenance);
                        if result.is_err() { probe.count("provenance_errors",1); }
                        result?;
                    }
                    yield InputBatch::update(variable.clone(),value);
                }
            }),
            stop,
        ))
    }
}

#[cfg(test)]
mod tests {
    use ecow::EcoVec;
    use futures::StreamExt;

    use super::*;
    use crate::core::{NewtypeSchema, UnionAlternative, UnionPayload, canonical_brand};
    use crate::io::config::ReplayStepConfig;
    use crate::io::group::GroupError;

    const BRAND: &str = "std::time::Instant<JournalTime>";

    fn fixture(name: &str) -> String {
        format!("{}/fixtures/rpio/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    /// A journal of the first `count` records of the service re-arm fixture.
    fn journal(count: usize) -> String {
        let text = std::fs::read_to_string(fixture("service-rearm.jsonl")).unwrap();
        let path = std::env::temp_dir().join(format!(
            "tc-observation-clock-{}-{}.jsonl",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let lines: Vec<_> = text.lines().take(count).collect();
        std::fs::write(&path, lines.join("\n") + "\n").unwrap();
        path.display().to_string()
    }

    fn steps(steps: &[(&[&str], Option<f64>)]) -> ReplaySteps {
        ReplaySteps::Inline(
            steps
                .iter()
                .map(|(members, time)| ReplayStepConfig {
                    step: members.iter().map(|member| (*member).to_owned()).collect(),
                    time: time.map(|time| serde_json::Number::from_f64(time).unwrap()),
                })
                .collect(),
        )
    }

    fn source(
        files: Vec<String>,
        clock_values: Vec<f64>,
        steps: Option<ReplaySteps>,
    ) -> FiniteObservationClock {
        FiniteObservationClock {
            binding: ObservationBinding {
                variable: VarName::new("observation"),
                domain: Some("JournalTime".into()),
                journal_clock: "fixture.journal".into(),
                declared: None,
            },
            files,
            capture_checks: Vec::new(),
            clock_values,
            clock_samples: None,
            steps,
            provenance: None,
            reading: Default::default(),
        }
    }

    fn observations(source: &FiniteObservationClock) -> Vec<Value> {
        source
            .materialise()
            .unwrap()
            .into_iter()
            .map(|tick| observation(tick.writes))
            .collect()
    }

    fn present<'a>(observation: &'a Value, field: &str) -> Option<&'a Value> {
        let Value::Map(fields) = observation else {
            panic!("an observation is a struct: {observation:?}")
        };
        let Value::Union(option) = &fields[field] else {
            panic!("`{field}` is an Option: {observation:?}")
        };
        match option.tag().as_str() {
            "None" => None,
            "Some" => option.payload(),
            other => panic!("`{field}` is tagged `{other}`"),
        }
    }

    fn instant(value: &Value) -> f64 {
        let Value::Newtype(instant) = value else {
            panic!("an instant is a newtype value: {value:?}")
        };
        assert_eq!(instant.brand(), BRAND);
        let Value::Float(seconds) = instant.value() else {
            panic!("an instant holds Float seconds")
        };
        *seconds
    }

    /// The record's admission instant, after checking that it is the
    /// record's own `arrival.journal_time`.
    fn record_time(observation: &Value) -> Option<f64> {
        let Value::Map(timed) = present(observation, "record")? else {
            panic!("a timed record is a struct")
        };
        let time = instant(&timed["time"]);
        assert_eq!(
            JournalArrival::<SecondsDomain>::try_from(&timed["value"])
                .unwrap()
                .0,
            Some(time)
        );
        Some(time)
    }

    fn clock(observation: &Value) -> Option<ClockSample<SecondsDomain>> {
        let sample = present(observation, "clock")?;
        let Value::Map(fields) = sample else {
            panic!("a clock sample is a struct")
        };
        instant(&fields["time"]);
        Some(ClockSample::try_from(sample).unwrap())
    }

    fn replayed(time: f64) -> ClockSample<SecondsDomain> {
        ClockSample {
            clock: ClockId::new("fixture.journal"),
            time,
            origin: SampleOrigin::Replay,
        }
    }

    #[test]
    fn replay_samples_keep_the_captured_clock_identity() {
        let mut replay = source(vec![fixture("service-rearm.jsonl")], vec![21.0], None);
        replay.binding.journal_clock = "captured.redis.time".into();
        let values = observations(&replay);
        let samples: Vec<_> = values.iter().filter_map(clock).collect();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].clock.as_str(), "captured.redis.time");
        assert_eq!(samples[0].time, 21.0);
        assert_eq!(samples[0].origin, SampleOrigin::Replay);
    }

    #[test]
    fn records_and_samples_arrive_as_typed_observations() {
        let observations = observations(&source(
            vec![fixture("service-rearm.jsonl")],
            vec![20.0, 21.0],
            None,
        ));
        assert_eq!(observations.len(), 10);
        assert_eq!(
            observations.iter().map(record_time).collect::<Vec<_>>(),
            [0.0, 1.0, 3.0, 12.0, 13.0, 14.0, 15.0, 16.0]
                .map(Some)
                .into_iter()
                .chain([None, None])
                .collect::<Vec<_>>()
        );
        assert!(observations[..8].iter().all(|o| clock(o).is_none()));
        assert_eq!(
            observations[8..].iter().map(clock).collect::<Vec<_>>(),
            vec![Some(replayed(20.0)), Some(replayed(21.0))]
        );
    }

    /// TS-5 gate, at the source: a clock-only tick has `record = None`, so
    /// nothing downstream sees the previous record again.
    #[test]
    fn clock_only_ticks_carry_no_record() {
        let observations = observations(&source(
            vec![journal(3)],
            vec![4.0, 5.0, 6.0],
            Some(steps(&[
                (&["journal"], None),
                (&["journal"], None),
                (&["clock"], None),
                (&["clock"], None),
                (&["clock"], None),
                (&["journal"], None),
            ])),
        ));
        for tick in &observations[2..5] {
            assert_eq!(record_time(tick), None);
        }
        assert_eq!(
            observations[2..5].iter().map(clock).collect::<Vec<_>>(),
            [4.0, 5.0, 6.0].map(|time| Some(replayed(time)))
        );
        assert_eq!(record_time(&observations[5]), Some(3.0));
    }

    /// TS-5 gate, at the source: a combined step is one tick carrying the
    /// record exactly once and the sample.
    #[test]
    fn a_combined_step_is_one_tick_with_both() {
        let source = source(
            vec![journal(2)],
            vec![1.5],
            Some(steps(&[
                (&["journal"], None),
                (&["journal", "clock"], None),
            ])),
        );
        let ticks = source.materialise().unwrap();
        assert_eq!(ticks.len(), 2);
        assert_eq!(
            ticks[1]
                .origins
                .iter()
                .map(|origin| (origin.member.as_str(), origin.kind))
                .collect::<Vec<_>>(),
            [(JOURNAL, OriginKind::Phase), (CLOCK, OriginKind::Clock)]
        );
        let combined = observation(ticks[1].writes.clone());
        assert_eq!(record_time(&combined), Some(1.0));
        assert_eq!(clock(&combined), Some(replayed(1.5)));
    }

    /// TS-5 gate, at the source: omitting a record, `[J, C, J, C]` becomes
    /// `[J, C, C]`; the samples keep their identities, and the omitted
    /// record is never observed.
    #[test]
    fn an_omission_keeps_the_clock_identities() {
        let normal = observations(&source(
            vec![journal(2)],
            vec![0.5, 1.5],
            Some(steps(&[
                (&["journal"], None),
                (&["clock"], None),
                (&["journal"], None),
                (&["clock"], None),
            ])),
        ));
        let omitted = observations(&source(
            vec![journal(1)],
            vec![0.5, 1.5],
            Some(steps(&[
                (&["journal"], None),
                (&["clock"], None),
                (&["clock"], None),
            ])),
        ));
        let samples =
            |observations: &[Value]| -> Vec<_> { observations.iter().filter_map(clock).collect() };
        assert_eq!(samples(&omitted), samples(&normal));
        assert_eq!(
            omitted.iter().map(record_time).collect::<Vec<_>>(),
            [Some(0.0), None, None]
        );
    }

    #[test]
    fn a_step_time_is_the_held_now_and_a_backwards_record_is_admitted() {
        let ticks = observations(&source(
            vec![fixture("service-rearm.jsonl")],
            vec![2.0, 20.0],
            Some(steps(&[
                (&["journal"], Some(0.0)),
                (&["journal"], Some(1.0)),
                (&["clock"], Some(2.0)),
                (&["journal"], Some(3.0)),
                (&["journal", "clock"], Some(20.0)),
                // Admitted at 13 while "now" is 20: kept, and "now" holds.
                (&["journal"], Some(20.0)),
                (&["journal"], None),
                (&["journal"], None),
                (&["journal"], Some(20.0)),
            ])),
        ));
        assert_eq!(record_time(&ticks[5]), Some(13.0));

        let error = source(
            vec![fixture("service-rearm.jsonl")],
            vec![],
            Some(steps(&[
                (&["journal"], Some(0.0)),
                (&["journal"], Some(0.5)),
            ])),
        )
        .materialise()
        .unwrap_err();
        let error = error.downcast::<GroupError>().unwrap();
        assert!(matches!(error, GroupError::TimeMismatch { .. }), "{error}");

        // Knowledge records do not advance "now".
        let error = source(
            vec![fixture("nominal.jsonl")],
            vec![],
            Some(steps(&[(&["journal"], Some(0.0))])),
        )
        .materialise()
        .unwrap_err();
        assert!(error.to_string().contains("not yet set"), "{error}");
    }

    #[test]
    fn a_schedule_naming_no_such_member_is_refused_before_any_tick() {
        let error = source(
            vec![journal(1)],
            vec![],
            Some(steps(&[(&["journal", "sensor"], None)])),
        )
        .open()
        .err()
        .expect("an unknown member is refused at open");
        assert!(
            error.to_string().contains("`sensor` is not a member"),
            "{error}"
        );
    }

    #[test]
    fn provenance_names_every_origin_of_every_tick() {
        let path = std::env::temp_dir().join(format!(
            "tc-observation-provenance-{}-{}.jsonl",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let file = journal(2);
        let mut source = source(
            vec![file.clone()],
            vec![1.5],
            Some(steps(&[
                (&["journal"], None),
                (&["journal", "clock"], None),
            ])),
        );
        source.provenance = Some(path.display().to_string());
        smol::block_on(async {
            let mut stream = source.open().unwrap();
            while let Some(batch) = stream.next().await {
                batch.unwrap();
            }
        });
        let text = std::fs::read_to_string(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            lines,
            [
                serde_json::json!({"tick": 0, "origins": [
                    {"member": "journal", "kind": "phase", "cursor": format!("{file}:1"), "time": 0.0},
                ]}),
                serde_json::json!({"tick": 1, "origins": [
                    {"member": "journal", "kind": "phase", "cursor": format!("{file}:2"), "time": 1.0},
                    {"member": "clock", "kind": "clock", "clock": "fixture.journal", "time": 1.5},
                ]}),
            ]
        );
    }

    fn declared_observation(record_domain: &str, clock_domain: &str) -> StreamType {
        let instant = |domain: &str| {
            let marker =
                StreamType::Newtype(NewtypeSchema::new(domain, EcoVec::new(), StreamType::Unit));
            StreamType::Newtype(NewtypeSchema::new(
                canonical_brand(
                    crate::core::time::INSTANT_STEM,
                    std::slice::from_ref(&marker),
                ),
                EcoVec::from([marker]),
                StreamType::Float,
            ))
        };
        let option = |payload: StreamType| {
            StreamType::Union(
                crate::core::ClosedUnion::new([
                    UnionAlternative::new("None", UnionPayload::Nullary),
                    UnionAlternative::new("Some", UnionPayload::Of(payload)),
                ])
                .unwrap(),
            )
        };
        StreamType::Struct(
            EcoVec::from([
                (
                    "record".into(),
                    option(StreamType::Struct(
                        EcoVec::from([
                            ("time".into(), instant(record_domain)),
                            ("value".into(), StreamType::Any),
                        ]),
                        false,
                    )),
                ),
                (
                    "clock".into(),
                    option(StreamType::Struct(
                        EcoVec::from([
                            ("clock".into(), StreamType::Str),
                            ("time".into(), instant(clock_domain)),
                            ("origin".into(), StreamType::Str),
                        ]),
                        false,
                    )),
                ),
            ]),
            false,
        )
    }

    #[test]
    fn the_declared_observation_type_binds_the_domain() {
        let mut binding = ObservationBinding {
            variable: VarName::new("observation"),
            domain: None,
            journal_clock: "fixture.journal".into(),
            declared: None,
        };
        let error = binding.domain().unwrap_err().to_string();
        assert!(error.contains("has no time domain"), "{error}");

        binding
            .bind_declared(&declared_observation(
                "app::JournalTime",
                "app::JournalTime",
            ))
            .unwrap();
        assert_eq!(
            binding.domain().unwrap().instant_brand(),
            "std::time::Instant<app::JournalTime>"
        );
        // Agreeing again is no change; a declaration without instants says
        // nothing.
        binding
            .bind_declared(&declared_observation(
                "app::JournalTime",
                "app::JournalTime",
            ))
            .unwrap();
        binding.bind_declared(&StreamType::Any).unwrap();
        assert_eq!(binding.domain.as_deref(), Some("app::JournalTime"));

        let error = format!(
            "{:#}",
            binding
                .bind_declared(&declared_observation("app::Producer", "app::Producer"))
                .unwrap_err()
        );
        assert!(
            error.contains("declared over time domain `app::Producer`")
                && error.contains("binds domain `app::JournalTime`"),
            "{error}"
        );
        let error = format!(
            "{:#}",
            binding
                .bind_declared(&declared_observation("app::JournalTime", "app::Producer"))
                .unwrap_err()
        );
        assert!(
            error.contains("clock instants in `app::Producer`"),
            "{error}"
        );
    }

    /// AC-TS-6: the group's tick k is stdout row k in both runtimes, even
    /// when row k prints nothing. A test gate input, present only on some of
    /// the group's ticks, silences the rest without adding or splitting a
    /// tick.
    #[test]
    fn a_silent_tick_still_takes_its_row_in_both_runtimes() {
        use std::cell::RefCell;
        use std::rc::Rc;

        use crate::core::{ExecutionPolicy, OutputInterface, Runtime, RuntimeSpec, Semantics};
        use crate::runtime::builder::GeneralRuntimeBuilder;

        const MODEL: &str = "\
use experimental::{modules, generics, tagged_unions, pattern_matching}
use std::option::{Option}
type Observation = Struct<record: Option<Struct<time: Any, value: Any>>,
                          clock: Option<Struct<clock: Str, time: Any, origin: Str>>>
in observation: Observation
in gate: Bool
out clocked: Bool = latch(std::option::is_some(observation.clock), gate)
";
        let gated = [1, 2, 5, 6];
        let plan = &[
            (&["journal"][..], None),
            (&["clock"], None),
            (&["journal"], None),
            (&["journal", "clock"], None),
            (&["clock"], None),
            (&["journal"], None),
            (&["clock"], None),
        ];
        for runtime in [
            RuntimeSpec::Dataflow(ExecutionPolicy::default()),
            RuntimeSpec::SemiSync,
        ] {
            let provenance = std::env::temp_dir().join(format!(
                "tc-observation-silent-{}-{}.jsonl",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            let file = journal(4);
            let mut source = source(
                vec![file.clone()],
                vec![0.5, 3.0, 4.0, 13.0],
                Some(steps(plan)),
            );
            source.provenance = Some(provenance.display().to_string());
            let mut observations = source.open().unwrap();
            let input: InputStream<Value> = Box::pin(async_stream::stream! {
                let mut tick = 0;
                while let Some(batch) = observations.next().await {
                    let mut batch = batch.unwrap();
                    if gated.contains(&tick) {
                        batch = InputBatch::tick(
                            batch
                                .into_ticks()
                                .next()
                                .unwrap()
                                .into_iter()
                                .chain([crate::core::InputUpdate::new(
                                    VarName::new("gate"),
                                    Value::Bool(true),
                                )])
                                .collect(),
                        )
                        .unwrap();
                    }
                    tick += 1;
                    yield Ok(batch);
                }
            });
            let printed = Rc::new(RefCell::new(Vec::<u8>::new()));
            let executor = Rc::new(smol::LocalExecutor::new());
            let run = {
                let executor = Rc::clone(&executor);
                let printed = Rc::clone(&printed);
                async move {
                    let writer = crate::io::output::open_stdout_with_shared_writer(
                        OutputInterface::outputs([VarName::new("clocked")]).unwrap(),
                        printed,
                    )
                    .await
                    .unwrap();
                    GeneralRuntimeBuilder::new()
                        .executor(Rc::clone(&executor))
                        .model(crate::dsrv_fixtures::elaborated(MODEL))
                        .input(input)
                        .output_writer(writer)
                        .runtime(runtime)
                        .semantics(Semantics::GradualTypedUntimed)
                        .build()
                        .await
                        .unwrap()
                        .run()
                        .await
                }
            };
            smol::block_on(executor.run(run)).unwrap();
            let lines = std::fs::read_to_string(&provenance).unwrap();
            let _ = std::fs::remove_file(&provenance);
            let _ = std::fs::remove_file(&file);
            let provenance: Vec<serde_json::Value> = lines
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(provenance.len(), plan.len(), "{runtime:?}");
            let expected: String = gated
                .iter()
                .map(|&k| {
                    let line = &provenance[k];
                    assert_eq!(line["tick"], k);
                    let clocked = line["origins"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|origin| origin["kind"] == "clock");
                    format!("clocked[{k}] = {clocked}\n")
                })
                .collect();
            assert_eq!(
                String::from_utf8(printed.borrow().clone()).unwrap(),
                expected,
                "{runtime:?}"
            );
        }
    }
    #[cfg(feature = "redis")]
    #[test]
    fn a_live_sampled_source_rejects_a_different_clock_before_connecting() {
        smol::block_on(async {
            let source = LiveObservationClock {
                binding: ObservationBinding {
                    variable: VarName::new("observation"),
                    domain: Some("JournalTime".into()),
                    journal_clock: "another.clock".into(),
                    declared: None,
                },
                host: "invalid.invalid".into(),
                port: Some(1),
                database: 0,
                phase_stream: "phase".into(),
                knowledge_stream: "knowledge".into(),
                period: Some(std::time::Duration::from_millis(10)),
                provenance: None,
            };
            let error = match source.open(Default::default()).await {
                Ok(_) => panic!("mismatched clock accepted"),
                Err(error) => error,
            };
            assert!(
                format!("{error:#}").contains("samples Redis server TIME"),
                "{error:#}"
            );
        });
    }
}
