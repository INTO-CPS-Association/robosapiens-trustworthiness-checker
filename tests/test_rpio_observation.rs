//! Native reference-journal input against a real Redis.
//!
//! Phase facts and immutable Knowledge commits use independent Redis streams.
//! Their IDs are opaque cursors, not Knowledge write counters or processing
//! positions. Redis envelopes bind exact record bytes to admission time/clock;
//! live projection equals replay, including Knowledge-first timestamp ties.
//! Frozen file manifests prove finite extent. Missing live evidence is checked
//! through exact references and causal publications, while shutdown drains only
//! a completed fetched snapshot. Periodic clocks cover both stream frontiers.

#![cfg(feature = "testcontainers")]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use anyhow::Context;
use futures::{FutureExt, StreamExt};
use macro_rules_attribute::apply;
use redis::AsyncCommands;
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use smol::LocalExecutor;

use tc_testutils::journal::app::{NativeCapture, Scratch, Trial};
use tc_testutils::redis::start_redis;
use tc_testutils::streams::with_timeout;
use trustworthiness_checker::async_test;
use trustworthiness_checker::core::REDIS_HOSTNAME;
use trustworthiness_checker::io::{InputConfigFile, InputPipeline, InputSources};
use trustworthiness_checker::{InputBatch, InputError, Value, VarName};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

struct NativeTrial {
    archive: Trial,
    capture: NativeCapture,
    lines: Vec<String>,
}

/// Port the preserved actual Redis capture through the same explicit native
/// converter used by the application-root integration assertions. The archive
/// stays byte-identical; inserted commits increase physical record count.
fn redis_trial() -> NativeTrial {
    let archive = Trial::load(&root().join("tests/fixtures/journal/app"), "nominal@redis");
    let schema = serde_json::from_slice(
        &std::fs::read(root().join("examples/screw-detection/observation_schema.json")).unwrap(),
    )
    .unwrap();
    let capture = NativeCapture::from_archive(&archive.journal(), &schema, archive.epoch());
    let lines = std::str::from_utf8(&capture.bytes)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    NativeTrial {
        archive,
        capture,
        lines,
    }
}

/// One tick's native resolved record and optional Redis clock sample. Record
/// order is measured by delivery count, independently of opaque source cursors.
#[derive(Debug, PartialEq)]
struct Tick {
    record: Option<Value>,
    clock: Option<(f64, String, String)>,
}

fn some(value: &Value) -> Option<&Value> {
    match value {
        Value::Union(option) if option.tag() == "Some" => option.payload(),
        _ => None,
    }
}

fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    let Value::Map(fields) = value else {
        panic!("expected a struct, got {value}")
    };
    &fields[name]
}

fn unwrap_newtype(value: &Value) -> &Value {
    match value {
        Value::Newtype(inner) => inner.value(),
        other => other,
    }
}

fn tick(batch: &InputBatch<Value>) -> Tick {
    let observation = &batch.updates().next().expect("one update").value;
    let record = some(field(observation, "record")).map(|timed| field(timed, "value").clone());
    let clock = some(field(observation, "clock")).map(|sample| {
        let Value::Float(time) = unwrap_newtype(field(sample, "time")) else {
            panic!("an instant")
        };
        let text = |name: &str| match field(sample, name) {
            Value::Str(text) => text.to_string(),
            other => panic!("{name}: {other}"),
        };
        (*time, text("clock"), text("origin"))
    });
    Tick { record, clock }
}

struct Live {
    port: u16,
    namespace: String,
    connection: redis::aio::MultiplexedConnection,
}

impl Live {
    async fn new(port: u16) -> anyhow::Result<Self> {
        let client = redis::Client::open(format!("redis://{REDIS_HOSTNAME}:{port}/0"))?;
        Ok(Self {
            port,
            namespace: format!("tc-reference-journal-{}", uuid::Uuid::new_v4()),
            connection: client.get_multiplexed_async_connection().await?,
        })
    }

    fn phase_key(&self) -> String {
        format!("{}:phase", self.namespace)
    }

    fn knowledge_key(&self) -> String {
        format!("{}:knowledge", self.namespace)
    }

    async fn add_raw(
        &mut self,
        knowledge: bool,
        id: &str,
        record: &str,
        time: &str,
    ) -> anyhow::Result<String> {
        let key = if knowledge {
            self.knowledge_key()
        } else {
            self.phase_key()
        };
        Ok(self
            .connection
            .xadd(
                key,
                id,
                &[
                    ("record", record),
                    ("journal_time", time),
                    ("journal_clock", "redis.server.time"),
                ],
            )
            .await?)
    }

    async fn add(&mut self, id: &str, record: &str) -> anyhow::Result<String> {
        let decoded: Json = serde_json::from_str(record)?;
        let time = decoded["payload"]["arrival"]["journal_time"].to_string();
        self.add_raw(decoded["$tag"] == "Knowledge", id, record, &time)
            .await
    }

    async fn add_all(&mut self, lines: &[String]) -> anyhow::Result<()> {
        for line in lines {
            self.add("*", line).await?;
        }
        Ok(())
    }

    async fn server_time(&mut self) -> anyhow::Result<f64> {
        let (seconds, micros): (u64, u64) =
            redis::cmd("TIME").query_async(&mut self.connection).await?;
        Ok(seconds as f64 + micros as f64 / 1e6)
    }

    fn config(&self, trial: &NativeTrial, extra: Json) -> InputConfigFile {
        serde_json::from_value(self.config_json(trial, extra)).unwrap()
    }

    fn config_json(&self, trial: &NativeTrial, extra: Json) -> Json {
        let mut source = json!({
            "kind": "rpio-observation-clock", "port": self.port,
            "variable": "observation", "domain": "JournalTime",
            "journal_clock": trial.archive.clock_label(),
            "phase_stream": self.phase_key(), "knowledge_stream": self.knowledge_key()
        });
        for (name, value) in extra.as_object().unwrap() {
            source[name] = value.clone();
        }
        json!({"sources": {"journal": source}})
    }
}

async fn open(
    config: InputConfigFile,
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<trustworthiness_checker::io::OpenedInput<Value>> {
    let sources = InputSources::<Value>::from_config(
        config,
        executor.clone(),
        None,
        None,
        Default::default(),
    )?;
    InputPipeline::from_sources(sources)
        .with_executor(executor)
        .build(BTreeSet::from([VarName::new("observation")]))
        .await
}

async fn next(
    input: &mut (impl futures::Stream<Item = Result<InputBatch<Value>, InputError>> + Unpin),
    label: &str,
) -> anyhow::Result<Option<Result<Tick, InputError>>> {
    Ok(with_timeout(input.next(), 10, label)
        .await?
        .map(|batch| batch.map(|batch| tick(&batch))))
}

fn file_config(trial: &NativeTrial, file: &Path, variable: &str) -> Json {
    json!({"sources": {"journal": {
        "kind": "rpio-observation-clock-file", "variable": variable,
        "domain": "JournalTime", "journal_clock": trial.archive.clock_label(),
        "files": [file.display().to_string()],
        "capture_checks": [{"records":trial.lines.len(),
            "sha256":format!("{:x}", Sha256::digest(&trial.capture.bytes)),
            "journal_clock":trial.archive.clock_label()}]
    }}})
}

async fn replayed(
    trial: &NativeTrial,
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<Vec<Value>> {
    let scratch = Scratch::new("native-live-replay");
    let file = scratch.write("capture.jsonl", &trial.capture.bytes);
    let config = serde_json::from_value(file_config(trial, &file, "observation"))?;
    let mut input = open(config, executor).await?;
    let mut records = Vec::new();
    while let Some(tick) = next(&mut input, "replay").await? {
        records.push(tick?.record.context("a replayed record")?);
    }
    Ok(records)
}

/// Only the configured Phase and Knowledge streams are read. Knowledge is
/// visible before its same-time Phase Write, and exact tuples match replay.
#[apply(async_test)]
async fn live_records_equal_their_archive_and_only_the_two_journal_streams_are_read(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let mut live = Live::new(container.get_host_port_ipv4(6379).await?).await?;
    let trial = redis_trial();
    let _: String = live
        .connection
        .xadd(
            format!("{}:journal", live.namespace),
            "1-1",
            &[("record", "not a record")],
        )
        .await?;
    let _: () = live
        .connection
        .set(format!("{}:ActiveModel", live.namespace), "{}")
        .await?;
    live.add_all(&trial.lines).await?;
    let expected = replayed(&trial, executor.clone()).await?;
    assert_eq!(expected.len(), trial.lines.len());
    assert!(
        expected
            .iter()
            .any(|v| matches!(v, Value::Union(r) if r.tag() == "Knowledge"))
    );
    let mut input = open(live.config(&trial, json!({})), executor).await?;
    let mut records = Vec::new();
    while records.len() < expected.len() {
        let tick = next(&mut input, "live record")
            .await?
            .context("live input ended")??;
        assert!(
            tick.clock.is_none(),
            "periodic clocks are explicitly omitted"
        );
        records.push(tick.record.context("a live record")?);
    }
    assert_eq!(records, expected);
    // Every new Write is immediately preceded in the native capture by its
    // immutable definition at the identical timestamp, despite independent IDs.
    for pair in records.windows(2) {
        if let Value::Union(phase) = &pair[1]
            && phase.tag() == "Phase"
            && matches!(field(phase.payload().unwrap(), "step"), Value::Union(step) if step.tag() == "Write")
        {
            let Value::Union(knowledge) = &pair[0] else {
                panic!("Knowledge before Write")
            };
            assert_eq!(knowledge.tag(), "Knowledge");
            assert_eq!(
                field(
                    field(knowledge.payload().unwrap(), "arrival"),
                    "journal_time"
                ),
                field(field(phase.payload().unwrap(), "arrival"), "journal_time")
            );
        }
    }
    Ok(())
}

/// A completed valid snapshot is delivered before a subsequent malformed
/// snapshot fails. No fact after the malformed entry can be mistaken for valid.
#[apply(async_test)]
async fn a_malformed_middle_entry_delivers_the_valid_prefix_then_an_error(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let mut live = Live::new(container.get_host_port_ipv4(6379).await?).await?;
    let trial = redis_trial();
    let expected = replayed(&trial, executor.clone()).await?;
    live.add_all(&trial.lines[..2]).await?;
    let mut input = open(live.config(&trial, json!({})), executor).await?;
    for record in &expected[..2] {
        assert_eq!(
            next(&mut input, "prefix")
                .await?
                .context("ended")??
                .record
                .as_ref(),
            Some(record)
        );
    }
    let time = serde_json::from_str::<Json>(&trial.lines[2])?["payload"]["arrival"]["journal_time"]
        .to_string();
    live.add_raw(false, "*", "{\"schema_version\":2,", &time)
        .await?;
    live.add_all(&trial.lines[3..6]).await?;
    let error = next(&mut input, "malformed")
        .await?
        .context("ended")?
        .unwrap_err();
    assert!(format!("{error}").contains("malformed JSON"), "{error}");
    assert!(next(&mut input, "after the error").await?.is_none());
    Ok(())
}

/// Redis IDs carry no numeric global-position contract. Valid arbitrary gaps
/// and non-1 suffixes must retain exactly the same decoded capture.
#[apply(async_test)]
async fn opaque_redis_cursors_are_independent_of_capture_and_write_positions(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let mut live = Live::new(container.get_host_port_ipv4(6379).await?).await?;
    let trial = redis_trial();
    for (index, line) in trial.lines.iter().enumerate() {
        live.add(&format!("{}-{}", 10_000 + index * 19, 7 + index * 3), line)
            .await?;
    }
    let expected = replayed(&trial, executor.clone()).await?;
    let mut input = open(live.config(&trial, json!({})), executor).await?;
    for record in expected {
        assert_eq!(
            next(&mut input, "opaque cursor")
                .await?
                .context("ended")??
                .record,
            Some(record)
        );
    }
    Ok(())
}

/// A native finite manifest preserves deletion/trimming/truncation detection
/// without treating opaque Redis IDs as contiguous positions. All altered
/// captures fail before producing even a valid-looking shorter prefix.
#[apply(async_test)]
async fn frozen_native_capture_rejects_trimmed_skipped_or_truncated_records(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let trial = redis_trial();
    let scratch = Scratch::new("native-extent-loss");
    let encode = |lines: &[String]| format!("{}\n", lines.join("\n")).into_bytes();
    let mut skipped = trial.lines.clone();
    skipped.remove(2);
    let mut edited = trial.lines.clone();
    edited[0] = format!(" {}", edited[0]);
    let mut extended = trial.capture.bytes.clone();
    extended.extend_from_slice(format!("{}\n", trial.lines.last().unwrap()).as_bytes());
    let mut truncated = trial.capture.bytes.clone();
    truncated.truncate(truncated.len() - 20);
    for (name, bytes) in [
        ("skipped", encode(&skipped)),
        ("trimmed", encode(&trial.lines[2..])),
        ("truncated", truncated),
        ("edited", encode(&edited)),
        ("extended", extended),
    ] {
        let file = scratch.write(&format!("{name}.jsonl"), bytes);
        let config = serde_json::from_value(file_config(&trial, &file, "observation"))?;
        let error = match open(config, executor.clone()).await {
            Err(error) => format!("{error:#}"),
            Ok(mut input) => {
                let error = next(&mut input, "frozen extent")
                    .await?
                    .context("missing source error")?
                    .unwrap_err();
                assert!(next(&mut input, "after extent error").await?.is_none());
                format!("{error}")
            }
        };
        assert!(
            error.contains("capture") || error.contains("SHA") || error.contains("newline"),
            "{name}: {error}"
        );
    }
    Ok(())
}

/// Omitting or trimming a required immutable definition cannot become a quiet
/// completed live prefix: shutdown reports the unresolved fetched references.
#[apply(async_test)]
async fn missing_live_knowledge_is_reported_at_drain(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let port = container.get_host_port_ipv4(6379).await?;
    let trial = redis_trial();
    let expected = replayed(&trial, executor.clone()).await?;
    let first_write: Json = trial
        .lines
        .iter()
        .find_map(|line| {
            let record: Json = serde_json::from_str(line).unwrap();
            (record["$tag"] == "Phase" && record["payload"]["step"]["$tag"] == "Write")
                .then_some(record)
        })
        .context("a callback Write")?;
    let reference = &first_write["payload"]["knowledge"]["payload"];
    let missing = trial
        .lines
        .iter()
        .position(|line| {
            let record: Json = serde_json::from_str(line).unwrap();
            record["$tag"] == "Knowledge" && &record["payload"]["version"]["ref"] == reference
        })
        .context("the exact immutable definition")?;
    assert!(missing > 0);
    for trim in [false, true] {
        let mut live = Live::new(port).await?;
        let mut missing_id = None;
        for (index, line) in trial.lines.iter().enumerate() {
            if !trim && index == missing {
                continue;
            }
            let id = live.add("*", line).await?;
            if index == missing {
                missing_id = Some(id);
            }
        }
        if let Some(id) = missing_id {
            let deleted: usize = redis::cmd("XDEL")
                .arg(live.knowledge_key())
                .arg(id)
                .query_async(&mut live.connection)
                .await?;
            assert_eq!(deleted, 1, "the required definition was actually trimmed");
        }
        let mut input = open(live.config(&trial, json!({})), executor.clone()).await?;
        for record in &expected[..missing] {
            assert_eq!(
                next(&mut input, "valid prefix before missing evidence")
                    .await?
                    .context("ended")??
                    .record
                    .as_ref(),
                Some(record)
            );
        }
        let mut drain = input.into_drain();
        let error = with_timeout(drain.next(), 5, "unresolved drain")
            .await?
            .context("missing incomplete-source error")?
            .unwrap_err();
        assert!(
            format!("{error}").contains("unresolved Knowledge"),
            "{error}"
        );
        assert!(drain.next().await.is_none());
    }
    Ok(())
}

/// Dropping a causal publication is still detected by the native wire contract,
/// independently of whether the Redis IDs have a numeric gap.
#[apply(async_test)]
async fn a_missing_live_causal_publication_stops_before_its_receiver(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let mut live = Live::new(container.get_host_port_ipv4(6379).await?).await?;
    let trial = redis_trial();
    let expected = replayed(&trial, executor.clone()).await?;
    let publication = trial
        .lines
        .iter()
        .position(|line| {
            let r: Json = serde_json::from_str(line).unwrap();
            r["$tag"] == "Phase" && r["payload"]["step"]["$tag"] == "Event"
        })
        .unwrap();
    live.add_all(&trial.lines[..publication]).await?;
    let mut input = open(live.config(&trial, json!({})), executor).await?;
    for record in &expected[..publication] {
        assert_eq!(
            next(&mut input, "causal prefix")
                .await?
                .context("ended")??
                .record
                .as_ref(),
            Some(record)
        );
    }
    live.add_all(&trial.lines[publication + 1..]).await?;
    let error = next(&mut input, "missing publication")
        .await?
        .context("ended")?
        .unwrap_err();
    assert!(format!("{error}").contains("never published"), "{error}");
    assert!(next(&mut input, "after causal error").await?.is_none());
    Ok(())
}

/// Samples cover both independent stream frontiers, including a record written
/// during sampling, while idle periods continue advancing Redis TIME.
#[apply(async_test)]
async fn clock_samples_come_from_redis_time_after_the_records_they_cover(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let mut live = Live::new(container.get_host_port_ipv4(6379).await?).await?;
    let trial = redis_trial();
    let expected = replayed(&trial, executor.clone()).await?;
    live.add_all(&trial.lines[..40]).await?;
    let before = live.server_time().await?;
    let mut input = open(live.config(&trial, json!({"clock_period_ms":5})), executor).await?;
    let mut delivered = 0;
    let mut clocks = Vec::new();
    let mut written = false;
    while clocks.len() < 6 || delivered < 41 {
        let tick = next(&mut input, "live tick").await?.context("ended")??;
        if let Some(record) = tick.record {
            assert_eq!(record, expected[delivered]);
            delivered += 1;
        }
        if let Some((time, label, origin)) = tick.clock {
            assert!(
                delivered >= 40,
                "clock released after only {delivered} records"
            );
            assert_eq!(label, "redis.server.time");
            assert_eq!(origin, "Periodic");
            clocks.push(time);
        }
        if clocks.len() == 3 && !written {
            live.add("*", &trial.lines[40]).await?;
            written = true;
        }
    }
    let after = live.server_time().await?;
    assert!(
        clocks.windows(2).all(|pair| pair[0] <= pair[1]),
        "{clocks:?}"
    );
    assert!(
        clocks.iter().all(|time| (before..=after).contains(time)),
        "{before} {clocks:?} {after}"
    );
    assert_eq!(delivered, 41);
    Ok(())
}

/// A completed snapshot is already fetched before its first record is emitted.
/// A later append must not be read during shutdown, with or without sampling.
#[apply(async_test)]
async fn shutdown_drains_buffered_records_without_new_reads(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let mut live = Live::new(container.get_host_port_ipv4(6379).await?).await?;
    let trial = redis_trial();
    let expected = replayed(&trial, executor.clone()).await?;
    live.add_all(&trial.lines[..40]).await?;
    let mut input = open(
        live.config(&trial, json!({"clock_period_ms":1000})),
        executor,
    )
    .await?;
    for record in &expected[..2] {
        assert_eq!(
            next(&mut input, "before shutdown")
                .await?
                .context("ended")??
                .record
                .as_ref(),
            Some(record)
        );
    }
    live.add_all(&trial.lines[40..]).await?;
    let mut drain = input.into_drain();
    let mut records = Vec::new();
    with_timeout(
        async {
            while let Some(batch) = drain.next().await {
                records.extend(tick(&batch?).record);
            }
            Ok::<_, InputError>(())
        },
        5,
        "drain",
    )
    .await??;
    assert_eq!(
        records,
        expected[2..40],
        "only the captured completed frontier is drained"
    );
    Ok(())
}

async fn quiet(
    input: &mut (impl futures::Stream<Item = Result<InputBatch<Value>, InputError>> + Unpin),
    wait: std::time::Duration,
) -> Option<Option<Result<Tick, InputError>>> {
    futures::select_biased! {
        item = input.next().fuse() => Some(item.map(|batch| batch.map(|batch| tick(&batch)))),
        _ = futures::FutureExt::fuse(smol::Timer::after(wait)) => None,
    }
}

#[apply(async_test)]
async fn silence_longer_than_a_blocking_read_is_neither_an_end_nor_an_error(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let port = container.get_host_port_ipv4(6379).await?;
    let trial = redis_trial();
    let expected = replayed(&trial, executor.clone()).await?;
    let silence = std::time::Duration::from_millis(2_500);
    let mut live = Live::new(port).await?;
    live.add_all(&trial.lines[..3]).await?;
    let mut input = open(live.config(&trial, json!({})), executor.clone()).await?;
    for record in &expected[..3] {
        assert_eq!(
            next(&mut input, "before silence")
                .await?
                .context("ended")??
                .record
                .as_ref(),
            Some(record)
        );
    }
    if let Some(item) = quiet(&mut input, silence).await {
        panic!("silence delivered {item:?}");
    }
    live.add_all(&trial.lines[3..6]).await?;
    for record in &expected[3..6] {
        let tick = next(&mut input, "after silence")
            .await?
            .context("ended")??;
        assert_eq!(tick.record.as_ref(), Some(record));
        assert!(tick.clock.is_none());
    }
    let mut live = Live::new(port).await?;
    live.add_all(&trial.lines[..3]).await?;
    let mut input = open(
        live.config(&trial, json!({"clock_period_ms":200})),
        executor,
    )
    .await?;
    let mut delivered = 0;
    let mut silent_samples = 0;
    let started = std::time::Instant::now();
    while started.elapsed() < silence || delivered < 3 {
        let tick = next(&mut input, "silence with a clock")
            .await?
            .context("ended")??;
        if let Some(record) = tick.record {
            assert_eq!(record, expected[delivered]);
            delivered += 1;
        } else if delivered == 3 {
            assert!(tick.clock.is_some());
            silent_samples += 1;
        }
    }
    assert!(
        silent_samples >= 5,
        "samples keep advancing through silence: {silent_samples}"
    );
    live.add_all(&trial.lines[3..6]).await?;
    while delivered < 6 {
        if let Some(record) = next(&mut input, "after silence with clock")
            .await?
            .context("ended")??
            .record
        {
            assert_eq!(record, expected[delivered]);
            delivered += 1;
        }
    }
    Ok(())
}

/// A snapshot can span several Redis read pages. Draining emits precisely that
/// completed snapshot and excludes later appends. Completeness is compared with
/// the frozen native capture's record count, never with opaque Redis ID parts.
#[apply(async_test)]
async fn the_consuming_drain_delivers_the_fetched_records_and_ends(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let port = container.get_host_port_ipv4(6379).await?;
    let mut trial = redis_trial();
    // The preserved nominal capture has fewer than 256 rows per stream. Add
    // valid idempotent Knowledge notices at their original timestamp so this
    // test must cover multiple pages of the Knowledge frontier. Their values
    // and exact references stay unchanged; they add physical ticks only.
    let first = trial.lines[0].clone();
    assert_eq!(serde_json::from_str::<Json>(&first)?["$tag"], "Knowledge");
    trial.lines.splice(1..1, vec![first; 260]);
    trial.capture.positions.splice(1..1, vec![None; 260]);
    trial.capture.bytes = format!("{}\n", trial.lines.join("\n")).into_bytes();
    let count = trial.lines.len();
    assert!(count > 512, "fixture exercises snapshot paging");
    let expected = replayed(&trial, executor.clone()).await?;
    assert_eq!(expected.len(), count);
    for (extra, written) in [
        (json!({}), 40),
        (json!({"clock_period_ms":1}), 40),
        (json!({}), count),
    ] {
        let mut live = Live::new(port).await?;
        live.add_all(&trial.lines[..written]).await?;
        let mut input = open(live.config(&trial, extra.clone()), executor.clone()).await?;
        let mut records = Vec::new();
        while records.is_empty() {
            records.extend(next(&mut input, "first").await?.context("ended")??.record);
        }
        if written < count {
            live.add_all(&trial.lines[written..]).await?;
        } else {
            live.add("*", &trial.lines[0]).await?;
        }
        let mut drain = input.into_drain();
        with_timeout(
            async {
                while let Some(batch) = drain.next().await {
                    records.extend(tick(&batch?).record);
                }
                Ok::<_, InputError>(())
            },
            10,
            "drain",
        )
        .await??;
        assert_eq!(
            records,
            expected[..written],
            "{extra} {written}: exact fetched snapshot"
        );
        assert_eq!(
            records.len() == count,
            written == count,
            "frozen capture completeness"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// A live checker process against the replay of the same capture.
// ---------------------------------------------------------------------------

type Rows = std::collections::BTreeMap<String, std::collections::BTreeMap<usize, String>>;

/// A checker process whose stdout rows are collected as they are printed.
struct LiveChecker {
    child: std::process::Child,
    lines: std::sync::mpsc::Receiver<String>,
    rows: Rows,
    scratch: tc_testutils::journal::app::Scratch,
}

impl LiveChecker {
    fn spawn(spec: &std::path::Path, config: &serde_json::Value, args: &[&str]) -> Self {
        use std::io::BufRead;
        let scratch = tc_testutils::journal::app::Scratch::new("live-checker");
        let config_path = scratch.write("input.json", serde_json::to_vec(config).unwrap());
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
            .arg(spec)
            .args([
                "--input-config",
                config_path.to_str().unwrap(),
                "--output-stdout",
            ])
            .args(args)
            .env("RUST_LOG", "error")
            .current_dir(root())
            .stdout(std::process::Stdio::piped())
            .stderr(std::fs::File::create(scratch.file("stderr.txt")).unwrap())
            .spawn()
            .expect("the checker starts");
        let stdout = child.stdout.take().unwrap();
        let (sender, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            lines,
            rows: Rows::new(),
            scratch,
        }
    }

    /// Collect rows until every output in `names` has printed `row`.
    fn wait_for_row(
        &mut self,
        names: &[String],
        row: usize,
        within: std::time::Duration,
    ) -> anyhow::Result<()> {
        let deadline = std::time::Instant::now() + within;
        while !names.iter().all(|name| {
            self.rows
                .get(name)
                .is_some_and(|rows| rows.contains_key(&row))
        }) {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            let line = self.lines.recv_timeout(left).map_err(|_| {
                anyhow::anyhow!(
                    "row {row} was not printed within {within:?}; checker status {:?}: {}",
                    self.child.try_wait(),
                    std::fs::read_to_string(self.scratch.file("stderr.txt")).unwrap_or_default()
                )
            })?;
            let Some((key, value)) = line.split_once(" = ") else {
                continue;
            };
            let Some((name, at)) = key.split_once('[') else {
                continue;
            };
            let Ok(at) = at.trim_end_matches(']').parse() else {
                continue;
            };
            self.rows
                .entry(name.to_owned())
                .or_default()
                .insert(at, value.to_owned());
        }
        Ok(())
    }

    fn running(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }
}

impl Drop for LiveChecker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Live and finite native inputs produce every output at every physical
/// record tick, including immutable definitions, without waiting for later
/// input. The completed capture is proved by the external frozen count.
#[apply(async_test)]
async fn a_live_checker_prints_the_replayed_values_through_the_final_capture_extent(
    _executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    use tc_testutils::journal::app::Checker;
    let container = start_redis().await;
    let mut live = Live::new(container.get_host_port_ipv4(6379).await?).await?;
    let trial = redis_trial();
    let spec = root().join("examples/screw-detection/sahar.dsrv");
    let count = trial.lines.len();
    let scratch = Scratch::new("native-live-agreement");
    let file = scratch.write("capture.jsonl", &trial.capture.bytes);
    let mut config = file_config(&trial, &file, "journal_input");
    config["sources"]["journal"]
        .as_object_mut()
        .unwrap()
        .remove("domain");
    config["sources"]["journal"]["provenance"] =
        json!(scratch.file("provenance.jsonl").display().to_string());
    let replay = Checker {
        binary: env!("CARGO_BIN_EXE_trustworthiness_checker").into(),
        cwd: root(),
    }
    .run(&spec, &config, "dataflow", 0);
    assert!(replay.success, "{}", replay.stderr);
    assert_eq!(replay.provenance.len(), count, "one tick per native record");
    let names: Vec<String> = replay.outputs.keys().cloned().collect();
    assert!(names.len() > 10, "{names:?}");
    for name in &names {
        assert_eq!(replay.rows(name), count, "{name}");
    }
    let mut config = live.config_json(&trial, json!({"variable":"journal_input"}));
    config["sources"]["journal"]
        .as_object_mut()
        .unwrap()
        .remove("domain");
    let mut checker = LiveChecker::spawn(
        &spec,
        &config,
        &["--runtime", "dataflow", "--execution-policy", "synchronous"],
    );
    let within = std::time::Duration::from_secs(120);
    let half = count / 2;
    live.add_all(&trial.lines[..half]).await?;
    checker.wait_for_row(&names, half - 1, within)?;
    smol::Timer::after(std::time::Duration::from_millis(2_500)).await;
    assert!(checker.running(), "silence does not stop the live checker");
    live.add_all(&trial.lines[half..]).await?;
    checker.wait_for_row(&names, count - 1, within)?;
    for name in &names {
        for row in 0..count {
            assert_eq!(
                checker.rows[name].get(&row),
                Some(replay.at(name, row)).map(str::to_owned).as_ref(),
                "{name}[{row}] native record {}",
                row + 1
            );
        }
    }
    assert!(checker.running());
    assert!(
        std::fs::read_to_string(checker.scratch.file("stderr.txt"))?
            .trim()
            .is_empty()
    );
    Ok(())
}
