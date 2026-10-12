//! Chrome trace event format export, readable by Perfetto (`ui.perfetto.dev`) and
//! `chrome://tracing`.
//!
//! Every [`TimelineTrack`] becomes one named thread of a single process. Spans are complete
//! (`"ph": "X"`) events and instants are thread-scoped (`"ph": "i"`) events; timestamps are
//! microseconds from the session epoch. Dropped entries are reported in the trace metadata.

use std::collections::BTreeMap;

use serde_json::{Value as Json, json};

use super::{Timeline, TimelineTrack};

/// Render `timelines` as one Chrome trace JSON document.
pub fn export<'a>(timelines: impl IntoIterator<Item = &'a Timeline>, metadata: Json) -> Json {
    let mut tracks = BTreeMap::<TimelineTrack, u64>::new();
    let mut events = Vec::new();
    let mut dropped = 0;
    for timeline in timelines {
        dropped += timeline.dropped();
        for event in timeline.events() {
            let next = tracks.len() as u64 + 1;
            let tid = *tracks.entry(event.track.clone()).or_insert(next);
            let mut entry = json!({
                "name": event.name,
                "cat": event.category,
                "pid": 1,
                "tid": tid,
                "ts": event.start_ns as f64 / 1000.0,
            });
            match event.duration_ns {
                Some(duration) => {
                    entry["ph"] = "X".into();
                    entry["dur"] = (duration as f64 / 1000.0).into();
                }
                None => {
                    entry["ph"] = "i".into();
                    entry["s"] = "t".into();
                }
            }
            if let Some(args) = &event.args {
                entry["args"] = args.clone();
            }
            events.push(entry);
        }
    }
    let mut trace_events = vec![json!({
        "name": "process_name", "ph": "M", "pid": 1, "tid": 0,
        "args": { "name": "trustworthiness-checker" }
    })];
    for (track, tid) in &tracks {
        trace_events.push(json!({
            "name": "thread_name", "ph": "M", "pid": 1, "tid": tid,
            "args": { "name": track.label() }
        }));
        trace_events.push(json!({
            "name": "thread_sort_index", "ph": "M", "pid": 1, "tid": tid,
            "args": { "sort_index": tid }
        }));
    }
    trace_events.extend(events);
    let mut metadata = metadata;
    if let Json::Object(fields) = &mut metadata {
        fields.insert("dropped_timeline_events".into(), dropped.into());
    }
    json!({
        "displayTimeUnit": "ns",
        "otherData": metadata,
        "traceEvents": trace_events,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::TimelineEvent;

    #[test]
    fn exports_spans_instants_and_track_names() {
        let mut timeline = Timeline::new(1);
        timeline.push(TimelineEvent {
            track: TimelineTrack::Pipeline,
            name: "input_wait".into(),
            category: "pipeline",
            start_ns: 2_000,
            duration_ns: Some(1_500),
            args: None,
        });
        timeline.push(TimelineEvent {
            track: TimelineTrack::Monitor,
            name: "dropped".into(),
            category: "monitor",
            start_ns: 0,
            duration_ns: None,
            args: None,
        });
        let trace = export([&timeline], json!({}));
        let events = trace["traceEvents"].as_array().unwrap();
        let span = events.iter().find(|event| event["ph"] == "X").unwrap();
        assert_eq!(span["ts"], 2.0);
        assert_eq!(span["dur"], 1.5);
        assert!(
            events
                .iter()
                .any(|event| event["name"] == "thread_name" && event["args"]["name"] == "pipeline")
        );
        assert_eq!(trace["otherData"]["dropped_timeline_events"], 1);
    }
}
