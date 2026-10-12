use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

/// Count, total and maximum of elapsed wall-clock durations, in nanoseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DurationStats {
    pub count: u64,
    pub total_ns: u64,
    pub max_ns: u64,
}

impl DurationStats {
    #[inline]
    pub fn record(&mut self, nanos: u64) {
        self.count += 1;
        self.total_ns = self.total_ns.saturating_add(nanos);
        self.max_ns = self.max_ns.max(nanos);
    }

    pub fn merge(&mut self, other: &Self) {
        self.count += other.count;
        self.total_ns = self.total_ns.saturating_add(other.total_ns);
        self.max_ns = self.max_ns.max(other.max_ns);
    }

    pub fn mean_ns(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total_ns as f64 / self.count as f64
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl Serialize for DurationStats {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("DurationStats", 4)?;
        state.serialize_field("count", &self.count)?;
        state.serialize_field("total_ns", &self.total_ns)?;
        state.serialize_field("mean_ns", &(self.mean_ns().round() as u64))?;
        state.serialize_field("max_ns", &self.max_ns)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_merges() {
        let mut stats = DurationStats::default();
        stats.record(10);
        stats.record(30);
        assert_eq!((stats.count, stats.total_ns, stats.max_ns), (2, 40, 30));
        assert_eq!(stats.mean_ns(), 20.0);
        let mut other = DurationStats::default();
        other.record(50);
        stats.merge(&other);
        assert_eq!((stats.count, stats.total_ns, stats.max_ns), (3, 90, 50));
        let json = serde_json::to_value(stats).unwrap();
        assert_eq!(json["mean_ns"], 30);
    }
}
