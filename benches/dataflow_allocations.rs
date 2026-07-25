use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};

use trustworthiness_checker::dataflow::DataflowMonitor;
use trustworthiness_checker::{DsrvSpecification, Value, VarName};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static REALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static REALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        REALLOCATED_BYTES.fetch_add(new_size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy)]
struct AllocationCounts {
    allocations: usize,
    allocated_bytes: usize,
    reallocations: usize,
    reallocated_bytes: usize,
}

fn reset_counts() {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    REALLOCATIONS.store(0, Ordering::Relaxed);
    REALLOCATED_BYTES.store(0, Ordering::Relaxed);
}

fn counts() -> AllocationCounts {
    AllocationCounts {
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
        allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        reallocations: REALLOCATIONS.load(Ordering::Relaxed),
        reallocated_bytes: REALLOCATED_BYTES.load(Ordering::Relaxed),
    }
}

fn compile(source: &str) -> DataflowMonitor {
    let spec = source
        .parse::<DsrvSpecification>()
        .expect("allocation benchmark specification should parse");
    DataflowMonitor::compile_untyped(spec).expect("allocation benchmark monitor should compile")
}

fn input_row(monitor: &DataflowMonitor, values: &[(&str, Value)]) -> Vec<Value> {
    monitor
        .input_vars()
        .iter()
        .map(|var| {
            values
                .iter()
                .find_map(|(name, value)| (var == &VarName::new(name)).then(|| value.clone()))
                .unwrap_or_else(|| panic!("missing allocation benchmark input `{var}`"))
        })
        .collect()
}

fn chain_spec(streams: usize, dynamic: bool) -> String {
    let mut source = String::from("in x: Int\nin dynamic_source: Str\n");
    for index in 0..streams - 1 {
        source.push_str(&format!("aux s{index}: Int\n"));
    }
    source.push_str("out result: Int\nout dynamic_result: Int\n");
    for index in 0..streams {
        let name = if index + 1 == streams {
            "result".to_owned()
        } else {
            format!("s{index}")
        };
        let operand = if index == 0 {
            "x".to_owned()
        } else {
            format!("s{}", index - 1)
        };
        source.push_str(&format!("{name} = {operand} + 1\n"));
    }
    if dynamic {
        source.push_str("dynamic_result = dynamic(dynamic_source: Int)\n");
    } else {
        source.push_str("dynamic_result = x\n");
    }
    source
}

fn first_class_spec(dynamic: bool) -> String {
    let scheduling = if dynamic {
        "a = dynamic(a_source: Int)\nb = dynamic(b_source: Int)"
    } else {
        "a = x\nb = x"
    };
    format!(
        "in x: Int\nin bias: Int\nin a_source: Str\nin b_source: Str\n\
         aux f: (Int -> Int)\nout z: Int\nout a: Int\nout b: Int\n\
         f = \\v: Int -> v[1] + bias\nz = f(x)\n{scheduling}"
    )
}

fn history_spec(offset: usize, recursive: bool) -> String {
    let delayed = if recursive {
        format!("default(delayed[{offset}], 0) + x")
    } else {
        format!("default(x[{offset}], 0)")
    };
    format!(
        "in x: Int\nin dynamic_source: Str\nout delayed: Int\nout dynamic_result: Int\n\
         delayed = {delayed}\n\
         dynamic_result = dynamic(dynamic_source: Int)\n"
    )
}

fn measure(
    name: &str,
    mut monitor: DataflowMonitor,
    input: Vec<Value>,
    ticks: usize,
) -> AllocationCounts {
    let mut output = vec![Value::NoVal; monitor.output_vars().len()];
    // Warm dynamic plans, lifting state, and retained histories before counting.
    monitor.evaluate(&input, &mut output).unwrap();
    monitor.evaluate(&input, &mut output).unwrap();
    reset_counts();
    for _ in 0..ticks {
        monitor.evaluate(black_box(&input), &mut output).unwrap();
    }
    let counts = counts();
    black_box(output);
    println!(
        "{name}: ticks={ticks}, allocations={}, allocations/tick={:.3}, allocated_bytes={}, bytes/tick={:.1}, reallocations={}, reallocated_bytes={}",
        counts.allocations,
        counts.allocations as f64 / ticks as f64,
        counts.allocated_bytes,
        counts.allocated_bytes as f64 / ticks as f64,
        counts.reallocations,
        counts.reallocated_bytes,
    );
    counts
}

fn main() {
    const TICKS: usize = 1_000;

    for streams in [1_usize, 8, 32, 128] {
        for mode in ["static", "dynamic"] {
            let dynamic = mode == "dynamic";
            let monitor = compile(&chain_spec(streams, dynamic));
            let input = input_row(
                &monitor,
                &[
                    ("x", Value::Int(1)),
                    ("dynamic_source", Value::Str("x".into())),
                ],
            );
            let counts = measure(&format!("chain/{mode}/{streams}"), monitor, input, TICKS);
            assert_eq!(counts.allocations, 0);
            assert_eq!(counts.allocated_bytes, 0);
            assert_eq!(counts.reallocations, 0);
            assert_eq!(counts.reallocated_bytes, 0);
        }
    }

    for dynamic in [false, true] {
        let monitor = compile(&first_class_spec(dynamic));
        let input = input_row(
            &monitor,
            &[
                ("x", Value::Int(2)),
                ("bias", Value::Int(20)),
                ("a_source", Value::Str("x".into())),
                ("b_source", Value::Str("a + 1".into())),
            ],
        );
        let mode = if dynamic { "dynamic" } else { "static" };
        measure(&format!("first_class/{mode}"), monitor, input, TICKS);
    }

    for recursive in [false, true] {
        for offset in [1_usize, 64, 1_024, 16_384] {
            let monitor = compile(&history_spec(offset, recursive));
            let input = input_row(
                &monitor,
                &[
                    ("x", Value::Int(1)),
                    ("dynamic_source", Value::Str("x".into())),
                ],
            );
            let kind = if recursive { "recursive" } else { "ordinary" };
            measure(
                &format!("history/{kind}_offset_{offset}"),
                monitor,
                input,
                TICKS,
            );
        }
    }
}
