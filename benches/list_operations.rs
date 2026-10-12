//! Time per tick of runtime list operations in the dataflow evaluator.
//!
//! The workloads are described in `list_operations/workloads.rs`. This binary uses jemalloc
//! without instrumentation; `list_operation_allocations` counts allocations for the same
//! workloads. Output: the value layout, then one JSON object per workload with the median time
//! per tick over several samples.
//!
//! Run with `cargo bench --profile bench-fast --features jemalloc --bench list_operations`,
//! optionally followed by substrings that select `element/workload/len` labels.

#[path = "list_operations/workloads.rs"]
mod workloads;

use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SAMPLES: usize = 7;
const SAMPLE_TIME: Duration = Duration::from_millis(40);

fn main() {
    println!("{}", workloads::layout());
    for mut workload in workloads::selected() {
        let start = Instant::now();
        let mut calibration = 0_usize;
        while start.elapsed() < SAMPLE_TIME / 4 {
            workload.tick();
            calibration += 1;
        }
        let ticks = (calibration * 4).max(1);
        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let start = Instant::now();
            for _ in 0..ticks {
                workload.tick();
            }
            samples.push(start.elapsed().as_nanos() as f64 / ticks as f64);
        }
        samples.sort_by(f64::total_cmp);
        let mut result = workload.label.clone();
        result["ticks_per_sample"] = ticks.into();
        result["samples"] = SAMPLES.into();
        result["median_ns_per_tick"] = samples[SAMPLES / 2].into();
        result["min_ns_per_tick"] = samples[0].into();
        result["max_ns_per_tick"] = samples[SAMPLES - 1].into();
        println!("{result}");
    }
}
