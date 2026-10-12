use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Instant;

use super::{
    DiagnosticsConfig, PipelinePhase, PipelineProfile, Timeline, TimelineEvent, TimelineTrack,
    nanos_since,
};

/// What one input source recorded.
#[derive(Debug)]
pub struct SourceRecord {
    pub name: String,
    pub profile: PipelineProfile,
    pub timeline: Timeline,
    /// Records or ticks delivered so far; selects which ones receive timeline spans.
    pub delivered: u64,
}

/// A shared handle through which one input source records its phases.
///
/// Input sources run inside the runtime's local executor, so the handle is a single-threaded
/// `Rc<RefCell<_>>`; each record is one short borrow.
#[derive(Clone, Debug)]
pub struct SourceRecorder {
    record: Rc<RefCell<SourceRecord>>,
    config: Rc<DiagnosticsConfig>,
}

impl SourceRecorder {
    /// Record `phase` as having run from `start` until now.
    pub fn phase(&self, phase: PipelinePhase, start: Instant) {
        let end = Instant::now();
        let nanos = nanos_since(start, end);
        let mut record = self.record.borrow_mut();
        record.profile.record(phase, nanos);
        if self.config.trace.ticks.contains(record.delivered) {
            let track = TimelineTrack::Input(record.name.clone());
            let start_ns = nanos_since(self.config.epoch, start);
            record.timeline.push_with(|| TimelineEvent {
                track,
                name: phase.name().into(),
                category: "input",
                start_ns,
                duration_ns: Some(nanos),
                args: None,
            });
        }
    }

    pub fn count(&self, name: &'static str, amount: u64) {
        self.record.borrow_mut().profile.counters.add(name, amount);
    }

    pub fn max(&self, name: &'static str, value: u64) {
        self.record.borrow_mut().profile.counters.max(name, value);
    }

    /// Mark one delivered tick, advancing the timeline selection.
    pub fn delivered(&self) {
        let mut record = self.record.borrow_mut();
        record.delivered += 1;
        record.profile.counters.add("delivered_ticks", 1);
    }
}

struct HubInner {
    config: Rc<DiagnosticsConfig>,
    sources: RefCell<Vec<Rc<RefCell<SourceRecord>>>>,
    /// Sources opened beyond `limits.max_sources`, whose records are not retained.
    sources_dropped: Cell<u64>,
}

/// The run-wide owner of a diagnostics configuration and of every source recorder.
///
/// An application creates one hub per run and hands clones to the input pipeline and to the
/// runtime builder. Sources register through [`Self::source_recorder`] when they open; the runtime
/// collects them when it writes the run's report. Nothing is global: a run without a hub records
/// nothing.
#[derive(Clone)]
pub struct DiagnosticsHub {
    inner: Rc<HubInner>,
}

impl DiagnosticsHub {
    pub fn new(config: DiagnosticsConfig) -> Self {
        Self {
            inner: Rc::new(HubInner {
                config: Rc::new(config),
                sources: RefCell::new(Vec::new()),
                sources_dropped: Cell::new(0),
            }),
        }
    }

    pub fn config(&self) -> &DiagnosticsConfig {
        &self.inner.config
    }

    /// Register a recorder for one source, named by its variable or description.
    pub fn source_recorder(&self, name: impl Into<String>) -> SourceRecorder {
        let record = Rc::new(RefCell::new(SourceRecord {
            name: name.into(),
            profile: PipelineProfile::default(),
            timeline: Timeline::new(self.inner.config.trace.capacity),
            delivered: 0,
        }));
        let mut sources = self.inner.sources.borrow_mut();
        if sources.len() < self.inner.config.limits.max_sources {
            sources.push(Rc::clone(&record));
        } else {
            self.inner
                .sources_dropped
                .set(self.inner.sources_dropped.get() + 1);
        }
        SourceRecorder {
            record,
            config: Rc::clone(&self.inner.config),
        }
    }

    /// Sources opened beyond the registry bound, whose records are not in the report.
    pub fn dropped_sources(&self) -> u64 {
        self.inner.sources_dropped.get()
    }

    /// Visit every registered source record.
    pub fn for_each_source(&self, mut visit: impl FnMut(&SourceRecord)) {
        for record in self.inner.sources.borrow().iter() {
            visit(&record.borrow());
        }
    }
}

impl std::fmt::Debug for DiagnosticsHub {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiagnosticsHub")
            .field("output_dir", &self.inner.config.output_dir)
            .finish_non_exhaustive()
    }
}
