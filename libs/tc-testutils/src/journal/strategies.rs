//! proptest strategies and independent reference models:
//!
//! - [`deadline`]: operation sequences for `std::timing::deadline` and a Rust
//!   restatement of it.

use proptest::prelude::*;

pub mod deadline {
    use super::*;

    pub type Key = u8;

    /// One call of `deadline(open, close, cancel, budget, {now, at})`;
    /// `budget` is fixed per run.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct DeadlineTick {
        pub open: Option<Key>,
        pub close: Option<Key>,
        pub cancel: Option<Key>,
        pub now_us: Option<i64>,
        /// `Some` on record ticks (the record's admission time), `None` on clock ticks.
        pub at_us: Option<i64>,
    }

    /// `Deadline<D>`; `completed` is `(start, end)`.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct DeadlineResult {
        pub overdue: bool,
        pub completed: Option<(i64, i64)>,
        pub pending: usize,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Wait {
        key: Key,
        since: i64,
        reported: bool,
    }

    /// `std::time::exceeds`: a forward span strictly longer than the budget.
    fn exceeds(span: Option<(i64, i64)>, budget: Option<i64>) -> bool {
        match (span, budget) {
            (Some((start, end)), Some(budget)) => end >= start && end - start > budget,
            _ => false,
        }
    }

    /// Restatement of `std::timing::deadline`: close and cancel before open,
    /// a duplicate open keeps its original time, each wait is reported
    /// overdue once.
    #[derive(Clone, Debug)]
    pub struct DeadlineModel {
        budget_us: Option<i64>,
        pending: Vec<Wait>,
    }

    impl DeadlineModel {
        pub fn new(budget_us: Option<i64>) -> Self {
            Self {
                budget_us,
                pending: vec![],
            }
        }

        pub fn step(&mut self, tick: &DeadlineTick) -> DeadlineResult {
            let budget = self.budget_us;
            let before = self.pending.clone();
            let acting = tick.at_us.is_some();
            let find = |waits: &[Wait], key: Option<Key>| {
                key.and_then(|k| waits.iter().find(|w| w.key == k).cloned())
            };
            let without = |waits: &[Wait], key: Option<Key>| -> Vec<Wait> {
                match key {
                    Some(k) => waits.iter().filter(|w| w.key != k).cloned().collect(),
                    None => waits.to_vec(),
                }
            };
            let response = if acting {
                find(&before, tick.close)
            } else {
                None
            };
            let answered = if acting {
                without(&before, tick.close)
            } else {
                before.clone()
            };
            let mut kept = if acting {
                without(&answered, tick.cancel)
            } else {
                answered
            };
            if acting && tick.open.is_some() && find(&kept, tick.open).is_none() {
                kept.push(Wait {
                    key: tick.open.unwrap(),
                    since: tick.at_us.unwrap(),
                    reported: false,
                });
            }
            let completion = match (&response, tick.at_us) {
                (Some(w), Some(at)) => Some((w.since, at)),
                _ => None,
            };
            let late_close =
                response.as_ref().is_some_and(|w| !w.reported) && exceeds(completion, budget);
            let age = |w: &Wait| tick.now_us.map(|now| (w.since, now));
            let late_open = kept.iter().any(|w| !w.reported && exceeds(age(w), budget));
            let next: Vec<Wait> = kept
                .iter()
                .map(|w| Wait {
                    reported: w.reported || exceeds(age(w), budget),
                    ..w.clone()
                })
                .collect();
            let result = DeadlineResult {
                overdue: late_close || late_open,
                completed: completion,
                pending: next.len(),
            };
            self.pending = next;
            result
        }
    }

    /// Run the model over a sequence.
    pub fn run(budget_us: Option<i64>, ticks: &[DeadlineTick]) -> Vec<DeadlineResult> {
        let mut model = DeadlineModel::new(budget_us);
        ticks.iter().map(|t| model.step(t)).collect()
    }

    /// Size bounds for [`ticks`].
    #[derive(Clone, Copy, Debug)]
    pub struct Bounds {
        pub max_len: usize,
        pub keys: Key,
        /// Allow raw record time below the held `now`.
        pub backwards_records: bool,
    }

    impl Default for Bounds {
        fn default() -> Self {
            Self {
                max_len: 40,
                keys: 5,
                backwards_records: true,
            }
        }
    }

    /// Time grid: multiples of 1/8 s, so DSRV float subtraction is exact and
    /// boundary cases (`span == budget`) are decided exactly.
    pub const GRID_US: i64 = 125_000;

    pub fn budget() -> impl Strategy<Value = Option<i64>> {
        prop_oneof![1 => Just(None), 4 => (0i64..=16).prop_map(|g| Some(g * GRID_US))]
    }

    #[derive(Clone, Debug)]
    enum Raw {
        Record {
            dt: i64,
            back: i64,
            open: Option<Key>,
            close: Option<Key>,
            cancel: Option<Key>,
        },
        Clock {
            dt: i64,
        },
    }

    /// Random open/close/cancel/clock sequences. `now` is the held maximum of
    /// record and clock times; record ticks may carry a raw time below `now`.
    /// Keys are `None` on clock ticks (the roots derive keys from record
    /// facts only).
    pub fn ticks(bounds: Bounds) -> impl Strategy<Value = Vec<DeadlineTick>> {
        let key = move || prop::option::weighted(0.6, 0..bounds.keys);
        let raw = prop_oneof![
            3 => (0i64..=8, 0i64..=3, key(), key(), prop::option::weighted(0.2, 0..bounds.keys)).prop_map(
                move |(dt, back, open, close, cancel)| Raw::Record {
                    dt,
                    back: if bounds.backwards_records { back } else { 0 },
                    open,
                    close,
                    cancel,
                }
            ),
            1 => (0i64..=16).prop_map(|dt| Raw::Clock { dt }),
        ];
        prop::collection::vec(raw, 1..=bounds.max_len).prop_map(move |raws| {
            let mut now: Option<i64> = None;
            let mut time = 0i64;
            raws.into_iter()
                .map(|raw| match raw {
                    Raw::Record {
                        dt,
                        back,
                        open,
                        close,
                        cancel,
                    } => {
                        time += dt * GRID_US;
                        // A backwards raw record time does not lower `now`.
                        let at = (time - back * GRID_US).max(0);
                        now = Some(now.map_or(at, |n| n.max(at)));
                        DeadlineTick {
                            open,
                            close,
                            cancel,
                            now_us: now,
                            at_us: Some(at),
                        }
                    }
                    Raw::Clock { dt } => {
                        time += dt * GRID_US;
                        now = Some(now.map_or(time, |n| n.max(time)));
                        DeadlineTick {
                            open: None,
                            close: None,
                            cancel: None,
                            now_us: now,
                            at_us: None,
                        }
                    }
                })
                .collect()
        })
    }
}
