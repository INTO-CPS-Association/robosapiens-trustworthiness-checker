//! `lazy_if` on the canonical dataflow path.
//!
//! Without the experiment an `if` is eager: both branches advance on every
//! tick, and either branch's absence or failure reaches the result. Under
//! `lazy_if` only the selected branch runs, on a timeline of its own that
//! advances only on the ticks that select it. Every case runs typed with
//! quickening, typed without it and untyped, which must agree.

use crate::core::{Semantics, Value};
use crate::dataflow::DataflowMonitor;
use crate::dsrv_fixtures::elaborated;

const LAZY: &str = "use experimental::lazy_if\n";

fn t() -> Value {
    Value::Bool(true)
}

fn f() -> Value {
    Value::Bool(false)
}

fn i(value: i64) -> Value {
    Value::Int(value)
}

/// Each canonical way of running `source`, by name.
fn monitors(source: &str) -> Vec<(&'static str, DataflowMonitor)> {
    let specification = elaborated(source);
    let quick = DataflowMonitor::compile_checked(specification.clone()).unwrap();
    let mut canonical = DataflowMonitor::compile_checked(specification.clone()).unwrap();
    canonical.set_quickening(false);
    let untyped =
        DataflowMonitor::compile_with_semantics(specification, Semantics::Untimed).unwrap();
    vec![
        ("typed", quick),
        ("typed canonical", canonical),
        ("untyped", untyped),
    ]
}

/// The only output of `source` over `rows`, which every way of running it
/// must agree on.
fn run(source: &str, rows: &[Vec<Value>]) -> Vec<Value> {
    let mut agreed: Option<Vec<Value>> = None;
    for (name, mut monitor) in monitors(source) {
        let outputs = rows
            .iter()
            .map(|row| {
                let mut output = [Value::NoVal];
                monitor
                    .evaluate(row, &mut output)
                    .unwrap_or_else(|error| panic!("{name}: {error}\n{source}"));
                output[0].clone()
            })
            .collect::<Vec<_>>();
        match &agreed {
            Some(agreed) => assert_eq!(&outputs, agreed, "{name} disagrees\n{source}"),
            None => agreed = Some(outputs),
        }
    }
    agreed.expect("at least one monitor")
}

/// `source` without a header, and under `lazy_if`.
fn eager_and_lazy(source: &str, rows: &[Vec<Value>]) -> (Vec<Value>, Vec<Value>) {
    (run(source, rows), run(&format!("{LAZY}{source}"), rows))
}

const DELAYED_THEN: &str = "in c: Bool\nin x: Int\nout y: Int\ny = if c then x[1] else 0\n";

#[test]
fn an_if_is_eager_unless_its_module_takes_on_lazy_if() {
    let rows = [vec![f(), i(1)], vec![f(), i(2)], vec![t(), i(3)]];
    // Eager: the then-branch advanced on the ticks that did not select it.
    assert_eq!(run(DELAYED_THEN, &rows), [i(0), i(0), i(2)]);
    // Every other experiment leaves `if` eager.
    let others = "use experimental::{tagged_unions, pattern_matching, generics, modules, \
                  functions, constants, casts}\n";
    assert_eq!(
        run(&format!("{others}{DELAYED_THEN}"), &rows),
        [i(0), i(0), i(2)]
    );
    // Lazy, whether named or through the umbrella: the first tick that
    // selects the branch is the first of its timeline.
    let lazy = [i(0), i(0), Value::Deferred];
    assert_eq!(run(&format!("{LAZY}{DELAYED_THEN}"), &rows), lazy);
    assert_eq!(
        run(
            &format!("use experimental::high_level_dsrv\n{DELAYED_THEN}"),
            &rows
        ),
        lazy
    );
}

#[test]
fn each_branch_advances_only_on_the_ticks_that_select_it() {
    let source = "in c: Bool\nin x: Int\nout y: Int\ny = if c then x[1] else x[1]\n";
    let rows = [
        vec![t(), i(1)],
        vec![f(), i(2)],
        vec![t(), i(3)],
        vec![f(), i(4)],
        vec![t(), i(5)],
    ];
    let (eager, lazy) = eager_and_lazy(source, &rows);
    assert_eq!(eager, [Value::Deferred, i(1), i(2), i(3), i(4)]);
    // The then-branch sees 1, 3, 5 and the else-branch 2, 4, so each is
    // Deferred the first time it is selected.
    assert_eq!(lazy, [Value::Deferred, Value::Deferred, i(1), i(2), i(3)]);
}

#[test]
fn branch_local_timelines_hold_on_the_fallible_path_too() {
    // Applying a lambda makes the stream's evaluation fallible.
    let source = "in c: Bool\nin x: Int\nout y: Int\n\
                  y = if c then (\\v: Int -> v)(x[1]) else (\\v: Int -> v)(x[1])\n";
    let rows = [
        vec![t(), i(1)],
        vec![f(), i(2)],
        vec![t(), i(3)],
        vec![f(), i(4)],
    ];
    let (eager, lazy) = eager_and_lazy(source, &rows);
    assert_eq!(eager, [Value::Deferred, i(1), i(2), i(3)]);
    assert_eq!(lazy, [Value::Deferred, Value::Deferred, i(1), i(2)]);
}

#[test]
fn an_absent_condition_runs_neither_branch() {
    let rows = [
        // No condition yet: nothing runs, and the result is absent.
        vec![Value::NoVal, i(1)],
        // The then-branch's first tick, so tick 0 did not advance it.
        vec![t(), i(2)],
        // Deferred is the result, and the branch does not see 3.
        vec![Value::Deferred, i(3)],
        vec![t(), i(4)],
        // A missing condition keeps the last one, which selects again.
        vec![Value::NoVal, i(5)],
    ];
    assert_eq!(
        run(&format!("{LAZY}{DELAYED_THEN}"), &rows),
        [Value::NoVal, Value::Deferred, Value::Deferred, i(2), i(4)]
    );
    assert_eq!(
        run(DELAYED_THEN, &rows),
        [Value::NoVal, i(1), Value::Deferred, i(3), i(4)]
    );
}

#[test]
fn an_unselected_branch_contributes_no_absence() {
    let source = "in c: Bool\nin x: Int\nin z: Int\nout y: Int\ny = if c then x else z\n";
    let rows = [
        vec![t(), i(1), Value::NoVal],
        vec![f(), i(2), i(5)],
        // The selected branch has no value, so it keeps its own last one.
        vec![t(), Value::NoVal, Value::NoVal],
    ];
    let (eager, lazy) = eager_and_lazy(source, &rows);
    // Eager: the else-branch's absence reaches the result, and the
    // then-branch retained what it saw on the tick that did not select it.
    assert_eq!(eager, [Value::NoVal, i(5), i(2)]);
    // Lazy: the then-branch saw 1, and nothing since.
    assert_eq!(lazy, [i(1), i(5), i(1)]);
}

#[test]
fn an_unselected_branch_cannot_fail() {
    let source = "in c: Bool\nin xs: List<Int>\nout y: Int\ny = if c then List.get(xs, 5) else 0\n";
    let rows = [vec![f(), Value::List(vec![i(1)].into())]];
    assert_eq!(run(&format!("{LAZY}{source}"), &rows), [i(0)]);
    // Eager evaluation runs the out-of-range read anyway.
    let mut monitor = DataflowMonitor::compile_checked(elaborated(source)).unwrap();
    let eager = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut output = [Value::NoVal];
        monitor.evaluate(&rows[0], &mut output).map(|()| output)
    }));
    assert!(eager.is_err(), "the eager read should fail: {eager:?}");
}

#[test]
fn a_recursive_delay_in_a_lazy_branch_reads_its_own_ticks() {
    let source = "in c: Bool\nout y: Int\ny = if c then default(y[1], 0) + 1 else 0\n";
    let rows = [vec![t()], vec![t()], vec![f()], vec![t()]];
    let (eager, lazy) = eager_and_lazy(source, &rows);
    // Eager: `y[1]` is the stream's previous tick.
    assert_eq!(eager, [i(1), i(2), i(0), i(1)]);
    // Lazy: the branch's own previous tick, so it keeps counting.
    assert_eq!(lazy, [i(1), i(2), i(0), i(3)]);
}

#[test]
fn a_recursive_function_still_stops_at_its_base_case() {
    let source = "in n: Int\nin bias: Int\nout z: Int\n\
                  z = fix(\\self: (Int -> Int), k: Int -> if k == 0 then bias else self(k - 1) + 1)(n)\n";
    let rows = [vec![i(0), i(10)], vec![i(3), i(10)], vec![i(5), i(2)]];
    let (eager, lazy) = eager_and_lazy(source, &rows);
    assert_eq!(eager, [i(10), i(13), i(7)]);
    assert_eq!(lazy, eager);
}

#[test]
fn nested_lazy_ifs_keep_nested_timelines() {
    let source = "in a: Bool\nin b: Bool\nin x: Int\nout y: Int\n\
                  y = if a then (if b then x[1] else 0) else 0\n";
    let rows = [
        vec![t(), f(), i(1)],
        vec![f(), t(), i(2)],
        vec![t(), t(), i(3)],
        vec![t(), t(), i(4)],
    ];
    let (eager, lazy) = eager_and_lazy(source, &rows);
    assert_eq!(eager, [i(0), i(0), i(2), i(3)]);
    // The inner then-branch runs first at tick 2.
    assert_eq!(lazy, [i(0), i(0), Value::Deferred, i(3)]);
}

#[test]
fn a_runtime_expression_in_a_lazy_branch_is_still_refused() {
    let source = format!(
        "{LAZY}in flag: Bool\nin x: Int\nin s: Str\nout z: Int\n\
         z = if flag then dynamic(s: Int) else x\n"
    );
    for semantics in [Semantics::Untimed, Semantics::TypedUntimed] {
        assert!(matches!(
            DataflowMonitor::compile_with_semantics(elaborated(&source), semantics),
            Err(crate::dataflow::DataflowCompilationError::UnsupportedReconfiguration { .. })
        ));
    }
}
