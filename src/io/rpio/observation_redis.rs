//! Live projection of an RPIO observation journal stored in Redis Streams.

use futures::future::LocalBoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use std::collections::VecDeque;
use std::future::Future;
use std::{
    cell::{Cell, RefCell},
    pin::Pin,
    rc::Rc,
    task::{Context as TaskContext, Poll},
};

use anyhow::Context;
use redis::{AsyncTypedCommands, streams::StreamReadOptions};

use ecow::EcoString;

use crate::core::time::{ClockId, ClockSample, SampleOrigin, SecondsDomain};
use crate::core::{InputBatch, InputError, InputStream, LocalStream, Value, VarName};
use crate::io::source_diagnostics::{SourcePhase, SourceProbe};

use super::{JournalEnvelope, JsonValue, KnowledgeResolver, ResolverError, WireDecoder};

const READ_COUNT: usize = 256;
const BLOCK_MILLISECONDS: usize = 1_000;
/// How long the connection waits for a reply. It must outlast a blocking
/// read of an idle stream, which replies only when the block ends.
const REPLY_TIMEOUT: std::time::Duration =
    std::time::Duration::from_millis(BLOCK_MILLISECONDS as u64 + 5_000);

#[derive(Debug)]
struct JournalEntry {
    cursor: String,
    journal_time: f64,
    journal_clock: EcoString,
    record: serde_json::Value,
    record_json: JsonValue,
    is_knowledge: bool,
}

/// One resolved journal record and the envelope of the entry that held it.
#[derive(Debug)]
pub(crate) struct LiveRecord {
    pub value: Value,
    /// The entry's Redis stream ID.
    pub cursor: EcoString,
    /// The label of the clock that stamped the entry's `journal_time`.
    pub journal_clock: EcoString,
    pub is_knowledge: bool,
}

#[derive(Default)]
struct LiveProjector {
    decoder: WireDecoder,
    resolver: KnowledgeResolver,
    pending: VecDeque<LiveRecord>,
}

impl LiveProjector {
    fn project(&mut self, entries: &[JournalEntry]) -> anyhow::Result<Vec<LiveRecord>> {
        for entry in entries {
            let tag = entry.record.get("$tag").and_then(serde_json::Value::as_str);
            anyhow::ensure!(
                matches!(
                    (entry.is_knowledge, tag),
                    (true, Some("Knowledge"))
                        | (false, Some("Phase"))
                        | (false, Some("Incomplete"))
                ),
                "RPIO observation record tag does not match its Redis stream"
            );
            let embedded = entry
                .record
                .pointer("/payload/arrival/journal_time")
                .and_then(serde_json::Value::as_f64)
                .context("RPIO observation record has no numeric arrival journal_time")?;
            anyhow::ensure!(
                embedded.is_finite() && embedded == entry.journal_time,
                "RPIO observation record journal_time differs from its Redis metadata"
            );
        }
        // Remember every Knowledge version in the reply before attempting
        // exact joins. This permits the two Redis streams to be delivered in
        // either order without changing their eventual record order.
        for entry in entries.iter().filter(|entry| entry.is_knowledge) {
            let value = self.decoder.knowledge_record(&entry.record_json)?;
            self.resolver.remember_json(&value, &entry.record_json)?;
        }
        for entry in entries {
            let value = if entry.is_knowledge {
                self.decoder.knowledge_record(&entry.record_json)?
            } else {
                self.decoder.phase_record(&entry.record_json)?
            };
            self.pending.push_back(LiveRecord {
                value,
                cursor: entry.cursor.as_str().into(),
                journal_clock: entry.journal_clock.clone(),
                is_knowledge: entry.is_knowledge,
            });
        }
        let mut ready = Vec::new();
        while let Some(record) = self.pending.front() {
            let resolved = match self.resolver.resolve(&record.value) {
                Ok(resolved) => resolved,
                Err(ResolverError::UnknownVersion { .. }) => break,
                Err(error) => return Err(error.into()),
            };
            let record = self.pending.pop_front().expect("front was present");
            ready.push(LiveRecord {
                value: resolved,
                ..record
            });
        }
        Ok(ready)
    }
}

fn decode_reply(
    reply: redis::streams::StreamReadReply,
    phase_stream: &str,
    knowledge_stream: &str,
) -> anyhow::Result<Vec<JournalEntry>> {
    let mut entries = Vec::new();
    for key in reply.keys {
        let is_knowledge = if key.key == knowledge_stream {
            true
        } else if key.key == phase_stream {
            false
        } else {
            anyhow::bail!(
                "Redis returned unexpected RPIO observation stream `{}`",
                key.key
            );
        };
        for id in key.ids {
            let field = |name: &str| -> anyhow::Result<String> {
                id.get(name).with_context(|| {
                    format!(
                        "RPIO observation {} entry `{}` has no valid `{name}` field",
                        key.key, id.id
                    )
                })
            };
            // The envelope is Redis text; a time that does not parse stays
            // text so the decoder reports it as the wrong type.
            let mut envelope = std::collections::BTreeMap::new();
            if let Some(text) = id.get::<String>("journal_time") {
                envelope.insert(
                    "journal_time".into(),
                    text.parse::<f64>()
                        .map_or_else(|_| Value::Str(text.into()), Value::Float),
                );
            }
            if let Some(clock) = id.get::<String>("journal_clock") {
                envelope.insert("journal_clock".into(), Value::Str(clock.into()));
            }
            let envelope = JournalEnvelope::<SecondsDomain>::try_from(&Value::Map(envelope.into()))
                .with_context(|| {
                    format!(
                        "RPIO observation {} entry `{}` has an invalid envelope",
                        key.key, id.id
                    )
                })?;
            let record_text = field("record")?;
            anyhow::ensure!(
                valid_id(&id.id),
                "RPIO observation entry has malformed Redis cursor `{}`",
                id.id
            );
            let record_json = JsonValue::parse(&record_text)?;
            let record = serde_json::from_str(&record_text).with_context(|| {
                format!(
                    "RPIO observation {} entry `{}` has invalid JSON in `record`",
                    key.key, id.id
                )
            })?;
            entries.push(JournalEntry {
                cursor: id.id,
                journal_time: envelope.time,
                journal_clock: envelope.clock,
                record,
                record_json,
                is_knowledge,
            });
        }
    }
    Ok(entries)
}

fn append_snapshot_entries(
    decoded: Vec<JournalEntry>,
    phase_high_water: &str,
    knowledge_high_water: &str,
    phase_cursor: &mut String,
    knowledge_cursor: &mut String,
    entries: &mut Vec<JournalEntry>,
) {
    for entry in decoded {
        let (high_water, cursor) = if entry.is_knowledge {
            (knowledge_high_water, &mut *knowledge_cursor)
        } else {
            (phase_high_water, &mut *phase_cursor)
        };
        // XREAD has no upper bound. Once this stream's captured high-water
        // entry has been included, leave concurrently appended entries for
        // the next wake rather than extending this snapshot indefinitely.
        if cursor == high_water {
            continue;
        }
        *cursor = entry.cursor.clone();
        entries.push(entry);
    }
}

/// Redis `TIME` and both evidence frontiers in one atomic operation. Sampling
/// uses a separate physical connection so an idle blocking read cannot delay it.
const SAMPLE: &str = r#"
local time = redis.call('TIME')
local function last(key)
  if redis.call('EXISTS', key) == 0 then return '0-0' end
  local info = redis.call('XINFO', 'STREAM', key)
  for i = 1, #info, 2 do
    if info[i] == 'last-generated-id' then return info[i + 1] end
  end
  return '0-0'
end
return {time[1], time[2], last(KEYS[1]), last(KEYS[2])}
"#;

pub(crate) const REDIS_SERVER_CLOCK: &str = "redis.server.time";

#[derive(Debug, Default)]
pub(crate) struct StopSignal {
    requested: Cell<bool>,
    waker: RefCell<Option<std::task::Waker>>,
}
impl StopSignal {
    pub(crate) fn stop(&self) {
        self.requested.set(true);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Cursors {
    phase: String,
    knowledge: String,
}
impl Cursors {
    fn zero() -> Self {
        Self {
            phase: "0-0".into(),
            knowledge: "0-0".into(),
        }
    }
    fn includes(&self, boundary: &Self) -> bool {
        id(&self.phase) >= id(&boundary.phase) && id(&self.knowledge) >= id(&boundary.knowledge)
    }
    fn accept(&mut self, record: &LiveRecord) {
        if record.is_knowledge {
            self.knowledge = record.cursor.to_string();
        } else {
            self.phase = record.cursor.to_string();
        }
    }
}
fn id(text: &str) -> (u64, u64) {
    let (a, b) = text.split_once('-').expect("validated Redis cursor");
    (
        a.parse().expect("validated Redis cursor"),
        b.parse().expect("validated Redis cursor"),
    )
}
fn valid_id(text: &str) -> bool {
    text.split_once('-')
        .is_some_and(|(a, b)| a.parse::<u64>().is_ok() && b.parse::<u64>().is_ok())
}

#[derive(Debug)]
struct ClockBoundary {
    sample: ClockSample<SecondsDomain>,
    through: Cursors,
}
fn boundary(reply: Vec<String>) -> Result<ClockBoundary, InputError> {
    let parsed = || -> Option<ClockBoundary> {
        let [seconds, micros, phase, knowledge] = reply.as_slice() else {
            return None;
        };
        if !valid_id(phase) || !valid_id(knowledge) {
            return None;
        }
        let micros = micros.parse::<u64>().ok()?;
        if micros >= 1_000_000 {
            return None;
        }
        Some(ClockBoundary {
            sample: ClockSample {
                clock: ClockId::new(REDIS_SERVER_CLOCK),
                time: seconds.parse::<u64>().ok()? as f64 + micros as f64 / 1e6,
                origin: SampleOrigin::Periodic,
            },
            through: Cursors {
                phase: phase.clone(),
                knowledge: knowledge.clone(),
            },
        })
    };
    parsed().ok_or_else(|| {
        InputError::backend(format!("RPIO Redis clock boundary is malformed: {reply:?}"))
    })
}

trait LiveTransport {
    fn poll_record(
        &mut self,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<Option<LiveRecord>, InputError>>;
    fn poll_clock(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<ClockBoundary, InputError>>;
    fn into_drain(self: Box<Self>) -> LocalStream<Result<LiveRecord, InputError>>;
}

#[derive(Debug)]
pub(crate) enum LiveTick {
    Record(LiveRecord),
    Clock(ClockSample<SecondsDomain>),
}
enum Phase {
    Live(Box<dyn LiveTransport>),
    Draining(LocalStream<Result<LiveRecord, InputError>>),
    Done,
}
struct OrderedLive {
    phase: Phase,
    delivered: Cursors,
    sampling: Option<(std::time::Duration, smol::Timer)>,
    requesting: bool,
    held: Option<ClockBoundary>,
    ready: VecDeque<LiveTick>,
    stop: Rc<StopSignal>,
    probe: SourceProbe,
}
impl OrderedLive {
    fn new(
        transport: Box<dyn LiveTransport>,
        period: Option<std::time::Duration>,
        stop: Rc<StopSignal>,
        probe: SourceProbe,
    ) -> Self {
        Self {
            phase: Phase::Live(transport),
            delivered: Cursors::zero(),
            sampling: period.map(|p| (p, smol::Timer::after(p))),
            requesting: false,
            held: None,
            ready: VecDeque::new(),
            stop,
            probe,
        }
    }
    fn release_held(&mut self) {
        if self
            .held
            .as_ref()
            .is_some_and(|b| self.delivered.includes(&b.through))
        {
            self.ready
                .push_back(LiveTick::Clock(self.held.take().expect("checked").sample));
        }
    }
    fn accept(&mut self, record: LiveRecord) {
        self.delivered.accept(&record);
        self.ready.push_back(LiveTick::Record(record));
        self.release_held();
    }
    fn fail(&mut self, error: InputError) -> Poll<Option<Result<LiveTick, InputError>>> {
        self.phase = Phase::Done;
        self.held = None;
        self.probe.count("source_errors", 1);
        Poll::Ready(Some(Err(error)))
    }
}
impl Stream for OrderedLive {
    type Item = Result<LiveTick, InputError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(tick) = this.ready.pop_front() {
                match &tick {
                    LiveTick::Record(_) => this.probe.count("record_ticks", 1),
                    LiveTick::Clock(_) => {
                        this.probe.count("clock_only_ticks", 1);
                        this.probe.count("clock_samples", 1);
                    }
                }
                return Poll::Ready(Some(Ok(tick)));
            }
            *this.stop.waker.borrow_mut() = Some(cx.waker().clone());
            if this.stop.requested.get() && matches!(this.phase, Phase::Live(_)) {
                let Phase::Live(transport) = std::mem::replace(&mut this.phase, Phase::Done) else {
                    unreachable!()
                };
                this.sampling = None;
                this.requesting = false;
                this.phase = Phase::Draining(transport.into_drain());
            }
            match &mut this.phase {
                Phase::Live(_) => {
                    if !this.requesting
                        && this.held.is_none()
                        && let Some((period, timer)) = &mut this.sampling
                        && Pin::new(&mut *timer).poll(cx).is_ready()
                    {
                        timer.set_after(*period);
                        this.requesting = true;
                    }
                    let Phase::Live(transport) = &mut this.phase else {
                        unreachable!()
                    };
                    if this.requesting {
                        match transport.poll_clock(cx) {
                            Poll::Ready(Ok(sample)) => {
                                this.requesting = false;
                                this.held = Some(sample);
                                this.release_held();
                            }
                            Poll::Ready(Err(error)) => return this.fail(error),
                            Poll::Pending => {}
                        }
                    }
                    let Phase::Live(transport) = &mut this.phase else {
                        unreachable!()
                    };
                    match transport.poll_record(cx) {
                        Poll::Ready(Ok(Some(record))) => this.accept(record),
                        Poll::Ready(Ok(None)) => {
                            this.phase = Phase::Done;
                            this.held = None;
                        }
                        Poll::Ready(Err(error)) => return this.fail(error),
                        Poll::Pending if this.ready.is_empty() => return Poll::Pending,
                        Poll::Pending => {}
                    }
                }
                Phase::Draining(records) => match records.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(record))) => this.accept(record),
                    Poll::Ready(Some(Err(error))) => return this.fail(error),
                    Poll::Ready(None) => {
                        this.phase = Phase::Done;
                        this.held = None;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Phase::Done => return Poll::Ready(None),
            }
        }
    }
}

/// A fetched page is not a proved merged prefix until both stream frontiers
/// of its atomic snapshot have been covered. Keep this proof outside the read
/// future so cancellation cannot turn a partial snapshot into a quiet drain.
#[derive(Default)]
struct FetchedSnapshot {
    entries: Vec<JournalEntry>,
    complete: bool,
}
impl FetchedSnapshot {
    fn take_complete(&mut self) -> Result<Vec<JournalEntry>, InputError> {
        let entries = std::mem::take(&mut self.entries);
        if !self.complete && !entries.is_empty() {
            return Err(InputError::source(format!(
                "RPIO fetched snapshot is incomplete: both stream high-water prefixes were not covered ({} entries withheld)",
                entries.len()
            )));
        }
        Ok(entries)
    }
}
fn drained_records(
    buffer: VecDeque<LiveRecord>,
    failure: Option<InputError>,
) -> LocalStream<Result<LiveRecord, InputError>> {
    Box::pin(futures::stream::iter(
        buffer.into_iter().map(Ok).chain(failure.map(Err)),
    ))
}

type Fetch = LocalBoxFuture<'static, Result<Cursors, InputError>>;
type Sample = LocalBoxFuture<'static, redis::RedisResult<Vec<String>>>;
struct RedisJournal {
    records: redis::aio::MultiplexedConnection,
    clock: redis::aio::MultiplexedConnection,
    phase_stream: String,
    knowledge_stream: String,
    fetched: Rc<RefCell<FetchedSnapshot>>,
    cursors: Cursors,
    first: bool,
    read: Option<Fetch>,
    sample: Option<Sample>,
    projector: LiveProjector,
    buffer: VecDeque<LiveRecord>,
    failure: Option<InputError>,
    probe: SourceProbe,
}
fn merge_entries(entries: &mut Vec<JournalEntry>) {
    // Snapshot pages append each stream in Redis-ID order. Keep that order
    // even when its wall clock goes backwards: only compare the two heads.
    let (knowledge, phase): (Vec<_>, Vec<_>) =
        entries.drain(..).partition(|entry| entry.is_knowledge);
    let mut knowledge = knowledge.into_iter().peekable();
    let mut phase = phase.into_iter().peekable();
    while let (Some(k), Some(p)) = (knowledge.peek(), phase.peek()) {
        if k.journal_time.total_cmp(&p.journal_time).is_le() {
            entries.push(knowledge.next().expect("head present"));
        } else {
            entries.push(phase.next().expect("head present"));
        }
    }
    entries.extend(knowledge);
    entries.extend(phase);
}
impl RedisJournal {
    async fn connect(
        host: String,
        port: Option<u16>,
        database: u32,
        phase_stream: String,
        knowledge_stream: String,
        probe: SourceProbe,
    ) -> anyhow::Result<Self> {
        let address = format!("redis://{}:{}/{database}", host, port.unwrap_or(6379));
        let client = redis::Client::open(address.as_str())
            .with_context(|| format!("invalid RPIO Redis address `{address}`"))?;
        let mut records = client.get_multiplexed_async_connection().await?;
        records.set_response_timeout(REPLY_TIMEOUT);
        let mut clock = client.get_multiplexed_async_connection().await?;
        clock.set_response_timeout(REPLY_TIMEOUT);
        Ok(Self {
            records,
            clock,
            phase_stream,
            knowledge_stream,
            fetched: Default::default(),
            cursors: Cursors::zero(),
            first: true,
            read: None,
            sample: None,
            projector: Default::default(),
            buffer: Default::default(),
            failure: None,
            probe,
        })
    }
    fn start_sample(&self) -> Sample {
        let mut connection = self.clock.clone();
        let phase = self.phase_stream.clone();
        let knowledge = self.knowledge_stream.clone();
        async move {
            redis::cmd("EVAL")
                .arg(SAMPLE)
                .arg(2)
                .arg(phase)
                .arg(knowledge)
                .query_async(&mut connection)
                .await
        }
        .boxed_local()
    }
    fn start_read(&mut self) -> Fetch {
        let mut connection = self.records.clone();
        let phase = self.phase_stream.clone();
        let knowledge = self.knowledge_stream.clone();
        let mut cursors = self.cursors.clone();
        let first = self.first;
        self.first = false;
        self.fetched.borrow_mut().complete = false;
        let fetched = Rc::clone(&self.fetched);
        let probe = self.probe.clone();
        async move {
            let keys = [phase.as_str(), knowledge.as_str()];
            let options = if first {
                StreamReadOptions::default().count(READ_COUNT)
            } else {
                StreamReadOptions::default()
                    .count(READ_COUNT)
                    .block(BLOCK_MILLISECONDS)
            };
            let reply: Option<redis::streams::StreamReadReply> = connection
                .xread_options(
                    &keys,
                    &[cursors.phase.as_str(), cursors.knowledge.as_str()],
                    &options,
                )
                .await
                .map_err(|e| InputError::backend(format!("RPIO Redis read failed: {e}")))?;
            let Some(reply) = reply else {
                return Ok(cursors);
            };
            let decode = probe.stamp();
            let decoded = decode_reply(reply, &phase, &knowledge).map_err(InputError::source)?;
            probe.phase(decode, SourcePhase::InputDecode);
            // Keep every fetched entry outside the pending future, so stop can
            // reject an unproved partial snapshot without fetching more.
            fetched.borrow_mut().entries.extend(decoded);
            let sample: Vec<String> = redis::cmd("EVAL")
                .arg(SAMPLE)
                .arg(2)
                .arg(&phase)
                .arg(&knowledge)
                .query_async(&mut connection)
                .await
                .map_err(|e| InputError::backend(format!("RPIO Redis snapshot failed: {e}")))?;
            let through = boundary(sample)?.through;
            let mut entries = std::mem::take(&mut fetched.borrow_mut().entries);
            let mut ordered = Vec::new();
            append_snapshot_entries(
                std::mem::take(&mut entries),
                &through.phase,
                &through.knowledge,
                &mut cursors.phase,
                &mut cursors.knowledge,
                &mut ordered,
            );
            fetched.borrow_mut().entries = ordered;
            while !cursors.includes(&through) {
                let options = StreamReadOptions::default().count(READ_COUNT);
                let reply: Option<redis::streams::StreamReadReply> = connection
                    .xread_options(
                        &keys,
                        &[cursors.phase.as_str(), cursors.knowledge.as_str()],
                        &options,
                    )
                    .await
                    .map_err(|e| {
                        InputError::backend(format!("RPIO Redis snapshot drain failed: {e}"))
                    })?;
                let reply = reply.ok_or_else(|| {
                    InputError::source("RPIO snapshot ended before both high-water cursors")
                })?;
                let decode = probe.stamp();
                let decoded =
                    decode_reply(reply, &phase, &knowledge).map_err(InputError::source)?;
                probe.phase(decode, SourcePhase::InputDecode);
                if decoded.is_empty() {
                    return Err(InputError::source("RPIO snapshot made no progress"));
                }
                append_snapshot_entries(
                    decoded,
                    &through.phase,
                    &through.knowledge,
                    &mut cursors.phase,
                    &mut cursors.knowledge,
                    &mut fetched.borrow_mut().entries,
                );
            }
            fetched.borrow_mut().complete = true;
            Ok(cursors)
        }
        .boxed_local()
    }
    fn project_fetched(&mut self) -> Result<(), InputError> {
        let mut fetched = self.fetched.borrow_mut().take_complete()?;
        merge_entries(&mut fetched);
        let stamp = self.probe.stamp();
        let result = self.projector.project(&fetched).map_err(InputError::source);
        self.probe.phase(stamp, SourcePhase::InputProjection);
        self.buffer.extend(result?);
        Ok(())
    }
}
impl LiveTransport for RedisJournal {
    fn poll_record(
        &mut self,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<Option<LiveRecord>, InputError>> {
        loop {
            if let Some(record) = self.buffer.pop_front() {
                return Poll::Ready(Ok(Some(record)));
            }
            if let Some(error) = self.failure.take() {
                return Poll::Ready(Err(error));
            }
            if self.read.is_none() {
                self.read = Some(self.start_read());
            }
            match self.read.as_mut().expect("started").as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.read = None;
                    match result {
                        Ok(cursors) => {
                            self.cursors = cursors;
                            self.project_fetched()?;
                        }
                        Err(error) => {
                            // Earlier completed snapshots are already in the
                            // output buffer. A partial new snapshot has no
                            // certified merged prefix: retain the primary
                            // error and explain why its fetched pages are held.
                            self.failure = Some(match self.project_fetched() {
                                Ok(()) => error,
                                Err(projection) => error.with_cleanup(projection),
                            });
                        }
                    }
                }
            }
        }
    }
    fn poll_clock(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<ClockBoundary, InputError>> {
        if self.sample.is_none() {
            self.sample = Some(self.start_sample())
        }
        let reply = match self.sample.as_mut().expect("started").as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(reply) => reply,
        };
        self.sample = None;
        Poll::Ready(boundary(reply.map_err(|e| {
            InputError::backend(format!("RPIO Redis clock sample failed: {e}"))
        })?))
    }
    fn into_drain(mut self: Box<Self>) -> LocalStream<Result<LiveRecord, InputError>> {
        self.read = None;
        self.sample = None;
        let result = self.project_fetched();
        let incomplete = result.err().or_else(|| {
            (!self.projector.pending.is_empty()).then(|| {
                InputError::source(
                    "RPIO shutdown leaves fetched records with unresolved Knowledge versions",
                )
            })
        });
        let pending = match (self.failure.take(), incomplete) {
            (Some(primary), Some(cleanup)) => Some(primary.with_cleanup(cleanup)),
            (primary, cleanup) => primary.or(cleanup),
        };
        drained_records(self.buffer, pending)
    }
}

pub(crate) async fn open_ticks(
    host: String,
    port: Option<u16>,
    database: u32,
    phase_stream: String,
    knowledge_stream: String,
    period: Option<std::time::Duration>,
    probe: SourceProbe,
) -> anyhow::Result<(LocalStream<Result<LiveTick, InputError>>, Rc<StopSignal>)> {
    let transport = RedisJournal::connect(
        host,
        port,
        database,
        phase_stream,
        knowledge_stream,
        probe.clone(),
    )
    .await?;
    let stop = Rc::new(StopSignal::default());
    Ok((
        Box::pin(OrderedLive::new(
            Box::new(transport),
            period,
            Rc::clone(&stop),
            probe,
        )),
        stop,
    ))
}

pub(crate) async fn open(
    host: String,
    port: Option<u16>,
    database: u32,
    phase_stream: String,
    knowledge_stream: String,
    variable: VarName,
    probe: SourceProbe,
) -> anyhow::Result<(InputStream<Value>, Rc<StopSignal>)> {
    let (mut ticks, stop) = open_ticks(
        host,
        port,
        database,
        phase_stream,
        knowledge_stream,
        None,
        probe,
    )
    .await?;
    Ok((
        Box::pin(
            async_stream::stream! {while let Some(tick)=ticks.next().await{yield tick.map(|tick|match tick {LiveTick::Record(record)=>InputBatch::update(variable.clone(),record.value),LiveTick::Clock(_)=>unreachable!("sampling disabled")});}},
        ),
        stop,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    fn entry(cursor: u64, time: f64, knowledge: bool) -> JournalEntry {
        JournalEntry {
            cursor: format!("{cursor}-0"),
            journal_time: time,
            journal_clock: REDIS_SERVER_CLOCK.into(),
            record: serde_json::Value::Null,
            record_json: JsonValue::parse("null").unwrap(),
            is_knowledge: knowledge,
        }
    }

    #[test]
    fn snapshot_ties_put_knowledge_before_phase_and_keep_stream_order() {
        let mut entries = vec![
            entry(1, 11., false),
            entry(2, 12., false),
            entry(6, 12., true),
            entry(3, 12., false),
            entry(7, 12., true),
        ];
        merge_entries(&mut entries);
        assert_eq!(
            entries
                .iter()
                .map(|e| e.cursor.as_str())
                .collect::<Vec<_>>(),
            ["1-0", "6-0", "7-0", "2-0", "3-0"]
        );
    }

    #[test]
    fn regressing_times_keep_stream_fifo_and_clock_waits_for_both_prefixes() {
        // Both clocks step backwards. A whole-snapshot time sort would move
        // Phase 11 before Phase 10 and Knowledge 21 before Knowledge 20.
        let mut entries = vec![
            entry(10, 5., false),
            entry(20, 3., true),
            entry(11, 1., false),
            entry(21, 2., true),
        ];
        merge_entries(&mut entries);
        assert_eq!(
            entries
                .iter()
                .map(|e| e.cursor.as_str())
                .collect::<Vec<_>>(),
            ["20-0", "21-0", "10-0", "11-0"]
        );
        let records = entries
            .into_iter()
            .map(|e| LiveRecord {
                value: Value::Float(e.journal_time),
                cursor: e.cursor.into(),
                journal_clock: e.journal_clock,
                is_knowledge: e.is_knowledge,
            })
            .collect();
        let (transport, _, _) = fake(records, sample(11, 21));
        let source = OrderedLive::new(
            Box::new(transport),
            Some(std::time::Duration::ZERO),
            Rc::new(StopSignal::default()),
            SourceProbe::default(),
        );
        let ticks = block_on(source.take(5).collect::<Vec<_>>());
        for (tick, (cursor, time)) in
            ticks[..4]
                .iter()
                .zip([("20-0", 3.), ("21-0", 2.), ("10-0", 5.), ("11-0", 1.)])
        {
            let LiveTick::Record(record) = tick.as_ref().unwrap() else {
                panic!("clock overtook its captured record prefixes")
            };
            assert_eq!(record.cursor, cursor);
            assert_eq!(record.value, Value::Float(time));
        }
        let LiveTick::Clock(clock) = ticks[4].as_ref().unwrap() else {
            panic!("covered clock sample was not released")
        };
        assert_eq!(clock.clock.as_str(), REDIS_SERVER_CLOCK);
        assert_eq!(clock.time, 42.);
    }

    #[test]
    fn incomplete_snapshot_cannot_release_a_later_knowledge_page() {
        // Knowledge has reached its high water, but Phase's earlier pages
        // have not all arrived. Sorting the fetched subset cannot prove a
        // merged prefix; a stop/read failure must report incompleteness.
        let mut snapshot = FetchedSnapshot {
            entries: vec![entry(20, 20., true), entry(10, 10., false)],
            complete: false,
        };
        let error = snapshot.take_complete().unwrap_err();
        assert!(error.to_string().contains("snapshot is incomplete"));
        assert!(error.to_string().contains("2 entries withheld"));
        assert!(snapshot.entries.is_empty());
    }

    #[test]
    fn completed_snapshot_releases_every_fetched_entry_but_idle_stop_is_empty() {
        let mut snapshot = FetchedSnapshot {
            entries: vec![entry(20, 20., true), entry(10, 10., false)],
            complete: true,
        };
        assert_eq!(snapshot.take_complete().unwrap().len(), 2);
        assert!(
            FetchedSnapshot::default()
                .take_complete()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn incomplete_drain_preserves_completed_records_then_reports_error() {
        block_on(async {
            let records = drained_records(
                VecDeque::from([record(1, false), record(2, true)]),
                Some(InputError::source("fetched snapshot is incomplete")),
            )
            .collect::<Vec<_>>()
            .await;
            assert_eq!(records.len(), 3);
            assert_eq!(records[0].as_ref().unwrap().cursor.as_str(), "1-0");
            assert_eq!(records[1].as_ref().unwrap().cursor.as_str(), "2-0");
            assert!(
                records[2]
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains("incomplete")
            );
        });
    }

    fn record(cursor: u64, knowledge: bool) -> LiveRecord {
        LiveRecord {
            value: Value::Int(cursor as i64),
            cursor: format!("{cursor}-0").into(),
            journal_clock: REDIS_SERVER_CLOCK.into(),
            is_knowledge: knowledge,
        }
    }
    fn sample(phase: u64, knowledge: u64) -> ClockBoundary {
        ClockBoundary {
            sample: ClockSample {
                clock: ClockId::new(REDIS_SERVER_CLOCK),
                time: 42.,
                origin: SampleOrigin::Periodic,
            },
            through: Cursors {
                phase: format!("{phase}-0"),
                knowledge: format!("{knowledge}-0"),
            },
        }
    }
    struct Fake {
        records: VecDeque<LiveRecord>,
        boundary: Option<ClockBoundary>,
        reads: Rc<Cell<usize>>,
        samples: Rc<Cell<usize>>,
        continuous: bool,
        idle: bool,
        emitted: u64,
    }
    impl LiveTransport for Fake {
        fn poll_record(
            &mut self,
            _: &mut TaskContext<'_>,
        ) -> Poll<Result<Option<LiveRecord>, InputError>> {
            self.reads.set(self.reads.get() + 1);
            if self.continuous {
                self.emitted += 1;
                return Poll::Ready(Ok(Some(record(self.emitted, false))));
            }
            if self.idle && self.records.is_empty() {
                return Poll::Pending;
            }
            Poll::Ready(Ok(self.records.pop_front()))
        }
        fn poll_clock(
            &mut self,
            _: &mut TaskContext<'_>,
        ) -> Poll<Result<ClockBoundary, InputError>> {
            if !self.continuous && self.boundary.is_none() {
                return Poll::Pending;
            }
            self.samples.set(self.samples.get() + 1);
            Poll::Ready(Ok(self
                .boundary
                .take()
                .unwrap_or_else(|| sample(self.emitted + 1, 0))))
        }
        fn into_drain(self: Box<Self>) -> LocalStream<Result<LiveRecord, InputError>> {
            Box::pin(futures::stream::iter(self.records.into_iter().map(Ok)))
        }
    }
    fn fake(
        records: Vec<LiveRecord>,
        boundary: ClockBoundary,
    ) -> (Fake, Rc<Cell<usize>>, Rc<Cell<usize>>) {
        let reads = Rc::new(Cell::new(0));
        let samples = Rc::new(Cell::new(0));
        (
            Fake {
                records: records.into(),
                boundary: Some(boundary),
                reads: reads.clone(),
                samples: samples.clone(),
                continuous: false,
                idle: false,
                emitted: 0,
            },
            reads,
            samples,
        )
    }
    #[test]
    fn clocks_wait_for_both_streams_even_when_phase_is_already_delivered() {
        block_on(async {
            let (fake, _, _) = fake(
                vec![record(1, false), record(1, true), record(2, true)],
                sample(1, 2),
            );
            let mut input = OrderedLive::new(
                Box::new(fake),
                Some(std::time::Duration::ZERO),
                Default::default(),
                Default::default(),
            );
            let mut kinds = Vec::new();
            while let Some(tick) = input.next().await {
                kinds.push(match tick.unwrap() {
                    LiveTick::Record(r) => {
                        if r.is_knowledge {
                            "K"
                        } else {
                            "P"
                        }
                    }
                    LiveTick::Clock(_) => "C",
                });
            }
            assert_eq!(kinds, ["P", "K", "K", "C"]);
        });
    }
    #[test]
    fn a_continuously_ready_journal_does_not_starve_due_clock_samples() {
        block_on(async {
            let (mut fake, _, samples) = fake(vec![], sample(1, 0));
            fake.continuous = true;
            let mut input = OrderedLive::new(
                Box::new(fake),
                Some(std::time::Duration::ZERO),
                Default::default(),
                Default::default(),
            );
            let mut clocks = 0;
            for _ in 0..80 {
                if matches!(input.next().await.unwrap().unwrap(), LiveTick::Clock(_)) {
                    clocks += 1;
                }
            }
            assert!(
                clocks >= 20,
                "continuously ready records starved the clock: {clocks}"
            );
            assert!(samples.get() >= clocks);
        });
    }
    #[test]
    fn stop_drains_fetched_records_and_only_the_clock_they_cover() {
        block_on(async {
            for (through, expected_clocks) in [(2, 1), (3, 0)] {
                let (fake, reads, samples) =
                    fake(vec![record(1, false), record(2, false)], sample(through, 0));
                let stop = Rc::new(StopSignal::default());
                let mut input = OrderedLive::new(
                    Box::new(fake),
                    Some(std::time::Duration::ZERO),
                    stop.clone(),
                    Default::default(),
                );
                assert!(matches!(
                    input.next().await.unwrap().unwrap(),
                    LiveTick::Record(_)
                ));
                stop.stop();
                let reads_before = reads.get();
                let samples_before = samples.get();
                let mut drained = Vec::new();
                let mut clocks = 0;
                while let Some(tick) = input.next().await {
                    match tick.unwrap() {
                        LiveTick::Record(r) => drained.push(r.cursor.to_string()),
                        LiveTick::Clock(_) => clocks += 1,
                    }
                }
                assert_eq!(drained, ["2-0"]);
                assert_eq!(clocks, expected_clocks);
                assert_eq!(reads.get(), reads_before, "drain fetched more records");
                assert_eq!(samples.get(), samples_before, "drain sampled another clock");
            }
        });
    }
    #[test]
    fn malformed_clock_boundaries_are_errors_and_numeric_cursor_order_is_used() {
        assert!(
            boundary(vec![
                "1".into(),
                "1000000".into(),
                "0-0".into(),
                "0-0".into()
            ])
            .is_err()
        );
        assert!(boundary(vec!["1".into(), "0".into(), "bad".into(), "0-0".into()]).is_err());
        assert!(
            Cursors {
                phase: "10-0".into(),
                knowledge: "9-0".into()
            }
            .includes(&Cursors {
                phase: "2-0".into(),
                knowledge: "9-0".into()
            })
        );
    }
    #[cfg(feature = "diagnostics")]
    #[test]
    fn diagnostics_count_records_and_clocks_without_adding_observations() {
        use crate::diagnostics::{DiagnosticsConfig, DiagnosticsHub};
        let run = |probe| {
            block_on(async {
                let (fake, _, _) = fake(vec![record(1, false), record(1, true)], sample(1, 1));
                OrderedLive::new(
                    Box::new(fake),
                    Some(std::time::Duration::ZERO),
                    Default::default(),
                    probe,
                )
                .map(|tick| format!("{:?}", tick.unwrap()))
                .collect::<Vec<_>>()
                .await
            })
        };
        let plain = run(Default::default());
        let hub = DiagnosticsHub::new(DiagnosticsConfig::default());
        let traced = run(SourceProbe::attached(hub.source_recorder("journal")));
        assert_eq!(plain, traced);
        hub.for_each_source(|record| {
            assert_eq!(record.profile.counters.count("record_ticks"), 2);
            assert_eq!(record.profile.counters.count("clock_only_ticks"), 1);
            assert_eq!(record.profile.counters.count("clock_samples"), 1);
            // Generic InputPipeline owns delivery accounting; the native
            // engine counts kinds without double-counting those deliveries.
            assert_eq!(record.profile.counters.count("delivered_ticks"), 0);
        });
    }
    #[test]
    fn stopping_an_idle_source_wakes_it_and_ends_without_another_read() {
        block_on(async {
            let (mut fake, reads, _) = fake(vec![], sample(0, 0));
            fake.idle = true;
            let stop = Rc::new(StopSignal::default());
            let mut input =
                OrderedLive::new(Box::new(fake), None, stop.clone(), Default::default());
            assert!(futures::poll!(input.next()).is_pending());
            let before = reads.get();
            stop.stop();
            assert!(input.next().await.is_none());
            assert_eq!(reads.get(), before);
        });
    }
}
