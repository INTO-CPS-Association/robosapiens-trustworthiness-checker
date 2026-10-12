//! Allocations per tick of runtime list operations in the dataflow evaluator.
//!
//! Counts the allocations, reallocations and requested bytes of the `list_operations`
//! workloads (see `list_operations/workloads.rs`) through a counting wrapper around jemalloc.
//! The wrapper adds work to every allocation, so this binary reports no times; use
//! `list_operations` for those.
//!
//! Run with `cargo bench --profile bench-fast --features jemalloc --bench
//! list_operation_allocations`, optionally followed by label substrings.

#[path = "list_operations/workloads.rs"]
mod workloads;

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static REALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { tikv_jemallocator::Jemalloc.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { tikv_jemallocator::Jemalloc.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(new_size, Ordering::Relaxed);
        unsafe { tikv_jemallocator::Jemalloc.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const TICKS: usize = 1_000;

fn main() {
    println!("{}", workloads::layout());
    for mut workload in workloads::selected() {
        ALLOCATIONS.store(0, Ordering::Relaxed);
        ALLOCATED_BYTES.store(0, Ordering::Relaxed);
        REALLOCATIONS.store(0, Ordering::Relaxed);
        for _ in 0..TICKS {
            workload.tick();
        }
        let per_tick =
            |counter: &AtomicUsize| counter.load(Ordering::Relaxed) as f64 / TICKS as f64;
        let mut result = workload.label.clone();
        result["ticks"] = TICKS.into();
        result["allocations_per_tick"] = per_tick(&ALLOCATIONS).into();
        result["reallocations_per_tick"] = per_tick(&REALLOCATIONS).into();
        result["requested_bytes_per_tick"] = per_tick(&ALLOCATED_BYTES).into();
        println!("{result}");
    }
}
