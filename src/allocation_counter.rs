//! Counts the allocations a closure makes, for tests that check an operation's allocations.
//!
//! This module installs the global allocator for the whole unit-test binary. Counts are kept
//! per thread, so tests running in parallel do not see each other's allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// Allocations and reallocations made, and the bytes they requested.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Allocations {
    pub(crate) count: usize,
    pub(crate) bytes: usize,
}

thread_local! {
    static ALLOCATIONS: Cell<Allocations> = const { Cell::new(Allocations { count: 0, bytes: 0 }) };
}

fn record(bytes: usize) {
    // `try_with` because a thread may allocate while its thread-locals are being destroyed.
    let _ = ALLOCATIONS.try_with(|counts| {
        let Allocations {
            count,
            bytes: total,
        } = counts.get();
        counts.set(Allocations {
            count: count + 1,
            bytes: total + bytes,
        });
    });
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The allocations made by this thread while `f` runs, and its result. The result is returned
/// rather than dropped, since freeing is not counted anyway and the caller may inspect it.
pub(crate) fn allocations<T>(f: impl FnOnce() -> T) -> (Allocations, T) {
    let before = ALLOCATIONS.with(Cell::get);
    let result = f();
    let after = ALLOCATIONS.with(Cell::get);
    let made = Allocations {
        count: after.count - before.count,
        bytes: after.bytes - before.bytes,
    };
    (made, result)
}
