use std::borrow::Cow;

use serde::Serialize;

/// The trace row a timeline entry belongs to.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimelineTrack {
    /// Runtime adapter phases: input waits, row preparation, output packing and delivery.
    Pipeline,
    /// Monitor ticks and structural monitor events.
    Monitor,
    /// Physical execution steps of a monitor.
    Steps,
    /// One input source, named by its variable or label.
    Input(String),
}

impl TimelineTrack {
    pub fn label(&self) -> Cow<'_, str> {
        match self {
            Self::Pipeline => "pipeline".into(),
            Self::Monitor => "monitor".into(),
            Self::Steps => "monitor steps".into(),
            Self::Input(name) => format!("input {name}").into(),
        }
    }
}

/// One span (`duration_ns: Some`) or instant (`duration_ns: None`).
#[derive(Clone, Debug, Serialize)]
pub struct TimelineEvent {
    pub track: TimelineTrack,
    pub name: Cow<'static, str>,
    pub category: &'static str,
    pub start_ns: u64,
    pub duration_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<serde_json::Value>,
}

/// A bounded, append-only buffer of timeline entries.
///
/// When full it keeps the earliest entries and counts what it dropped, so the beginning of a run
/// is never silently replaced by its end.
#[derive(Clone, Debug)]
pub struct Timeline {
    capacity: usize,
    events: Vec<TimelineEvent>,
    dropped: u64,
}

impl Timeline {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            events: Vec::new(),
            dropped: 0,
        }
    }

    /// Whether another entry would be retained.
    #[inline]
    pub fn has_room(&self) -> bool {
        self.events.len() < self.capacity
    }

    /// Append an entry built by `event`, which is only called when there is room.
    #[inline]
    pub fn push_with(&mut self, event: impl FnOnce() -> TimelineEvent) {
        if self.has_room() {
            self.events.push(event());
        } else {
            self.dropped += 1;
        }
    }

    pub fn push(&mut self, event: TimelineEvent) {
        self.push_with(|| event);
    }

    pub fn events(&self) -> &[TimelineEvent] {
        &self.events
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Move every entry of `other` into this timeline, respecting this capacity.
    pub fn absorb(&mut self, other: Timeline) {
        self.dropped += other.dropped;
        for event in other.events {
            self.push(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instant(name: &'static str) -> TimelineEvent {
        TimelineEvent {
            track: TimelineTrack::Monitor,
            name: name.into(),
            category: "test",
            start_ns: 0,
            duration_ns: None,
            args: None,
        }
    }

    #[test]
    fn keeps_the_earliest_entries_and_counts_drops() {
        let mut timeline = Timeline::new(2);
        let mut built = 0;
        for name in ["a", "b", "c", "d"] {
            timeline.push_with(|| {
                built += 1;
                instant(name)
            });
        }
        assert_eq!(built, 2, "a dropped entry is never constructed");
        assert_eq!(
            timeline
                .events()
                .iter()
                .map(|event| event.name.as_ref())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(timeline.dropped(), 2);
    }
}
