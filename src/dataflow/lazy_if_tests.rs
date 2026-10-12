//! `lazy_if` on the canonical dataflow path.
//!
//! Without the experiment an `if` is eager: both branches advance on every
//! tick, and either branch's absence or failure reaches the result. Under
//! `lazy_if` only the selected branch runs, and what it computes has a
//! timeline of its own that advances only on the ticks that select it. A
//! previous value of a named stream, `x[n]`, is the exception: it reads that
//! stream's history, exactly as it would outside the `if`. The same holds in a
//! `match` arm or guard, which only ever runs when selected or tried, and in a
//! lambda written in either. Every case runs typed with quickening, typed
//! without it and untyped, which must agree.

use crate::core::{Semantics, Value};
use crate::dataflow::{
    DataflowCompilationError, DataflowEvaluationError, DataflowMonitor, StreamProgramError,
};
use crate::dsrv_fixtures::elaborated;

const LAZY: &str = "use experimental::lazy_if\n";
const MATCHING: &str = "use experimental::{tagged_unions, pattern_matching}\n";

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

/// A history the then-branch computes itself, rather than a stream's.
const COMPUTED_THEN: &str = "in c: Bool\nin x: Int\nout y: Int\ny = if c then (x + 0)[1] else 0\n";

#[test]
fn an_if_is_eager_unless_its_module_takes_on_lazy_if() {
    let source = "in c: Bool\nin x: Int\nin z: Int\nout y: Int\ny = if c then x else z\n";
    let rows = [vec![t(), i(1), Value::NoVal], vec![f(), i(2), i(3)]];
    // Eager: the unselected else-branch has no value yet, and that reaches the result.
    let eager = [Value::NoVal, i(3)];
    assert_eq!(run(source, &rows), eager);
    // Every other experiment leaves `if` eager.
    let others = "use experimental::{tagged_unions, pattern_matching, generics, modules, \
                  functions, constants, casts}\n";
    assert_eq!(run(&format!("{others}{source}"), &rows), eager);
    // Lazy, whether named directly or through the meta-feature: only the
    // selected branch contributes.
    let lazy = [i(1), i(3)];
    assert_eq!(run(&format!("{LAZY}{source}"), &rows), lazy);
    assert_eq!(
        run(
            &format!("use experimental::high_level_dsrv\n{source}"),
            &rows
        ),
        lazy
    );
}

#[test]
fn a_stream_read_in_a_lazy_branch_is_its_previous_tick() {
    let rows = [vec![f(), i(1)], vec![f(), i(2)], vec![t(), i(3)]];
    // The first tick that selects the branch still reads the tick before it.
    let (eager, lazy) = eager_and_lazy(DELAYED_THEN, &rows);
    assert_eq!(eager, [i(0), i(0), i(2)]);
    assert_eq!(lazy, eager);

    let source = "in c: Bool\nin x: Int\nout y: Int\ny = if c then x[1] else x[2]\n";
    let rows = [
        vec![t(), i(1)],
        vec![f(), i(2)],
        vec![t(), i(3)],
        vec![f(), i(4)],
        vec![t(), i(5)],
    ];
    let (eager, lazy) = eager_and_lazy(source, &rows);
    assert_eq!(eager, [Value::Deferred, Value::Deferred, i(2), i(2), i(4)]);
    assert_eq!(lazy, eager);
}

#[test]
fn a_recursive_read_in_a_lazy_branch_is_the_streams_previous_tick() {
    let source = "in c: Bool\nout y: Int\ny = if c then default(y[1], 0) + 1 else 0\n";
    let rows = [vec![t()], vec![t()], vec![f()], vec![t()]];
    let (eager, lazy) = eager_and_lazy(source, &rows);
    // `y[1]` is the stream's previous tick, which the else-branch produced.
    assert_eq!(eager, [i(1), i(2), i(0), i(1)]);
    assert_eq!(lazy, eager);
}

/// The trap this guards against: a previous value written inside a lazy
/// branch must mean what it means when read outside the `if` and passed in.
#[test]
fn a_previous_value_in_a_lazy_branch_reads_as_if_written_outside_it() {
    let rows = [
        vec![Value::NoVal, i(1)],
        vec![t(), i(2)],
        vec![f(), Value::NoVal],
        vec![Value::Deferred, i(4)],
        vec![t(), i(5)],
        vec![f(), i(6)],
        vec![t(), i(7)],
        vec![Value::NoVal, Value::NoVal],
        vec![f(), i(9)],
    ];
    let pairs = [
        // A counter that the else-branch keeps, as a streak does.
        (
            "y = if c then 0 else default(y[1], 0) + x",
            "aux p: Int = y[1]\ny = if c then 0 else default(p, 0) + x",
        ),
        // Another stream, a deeper offset, and an index of an index.
        (
            "y = if c then default(x[2], 0) else default((x[1])[1], 0) * 10",
            "aux p: Int = x[2]\naux q: Int = x[1]\naux r: Int = q[1]\n\
             y = if c then default(p, 0) else default(r, 0) * 10",
        ),
        // A nested lazy `if`, whose condition also reads a previous value.
        (
            "y = if c then (if default(x[1], 0) > 3 then default(y[1], 0) + 1 else 0) else -1",
            "aux p: Int = x[1]\naux q: Int = y[1]\n\
             y = if c then (if default(p, 0) > 3 then default(q, 0) + 1 else 0) else -1",
        ),
    ];
    for (inline, hoisted) in pairs {
        let header = format!("{LAZY}in c: Bool\nin x: Int\nout y: Int\n");
        let inline = run(&format!("{header}{inline}\n"), &rows);
        assert_eq!(inline, run(&format!("{header}{hoisted}\n"), &rows));
    }
    // The first pair by hand: the else-branch adds to whatever `y` was.
    let header = format!("{LAZY}in c: Bool\nin x: Int\nout y: Int\n");
    assert_eq!(
        run(&format!("{header}{}\n", pairs[0].0), &rows),
        [
            Value::NoVal,
            i(0),
            Value::NoVal,
            Value::Deferred,
            i(0),
            i(6),
            i(0),
            i(0),
            i(9),
        ]
    );
}

#[test]
fn a_computed_history_in_a_lazy_branch_keeps_the_branchs_timeline() {
    let source = "in c: Bool\nin x: Int\nout y: Int\ny = if c then (x + 0)[1] else (x + 0)[1]\n";
    let rows = [
        vec![t(), i(1)],
        vec![f(), i(2)],
        vec![t(), i(3)],
        vec![f(), i(4)],
        vec![t(), i(5)],
    ];
    let (eager, lazy) = eager_and_lazy(source, &rows);
    assert_eq!(eager, [Value::Deferred, i(1), i(2), i(3), i(4)]);
    // The then-branch computes 1, 3, 5 and the else-branch 2, 4, so each is
    // Deferred the first time it is selected.
    assert_eq!(lazy, [Value::Deferred, Value::Deferred, i(1), i(2), i(3)]);
}

#[test]
fn branch_timelines_hold_on_the_fallible_path_too() {
    // Applying a lambda makes the stream's evaluation fallible.
    let source = "in c: Bool\nin x: Int\nout y: Int\n\
                  y = if c then (\\v: Int -> v)(x[1]) + (x + 0)[1] \
                  else (\\v: Int -> v)(x[1]) + (x + 0)[1]\n";
    let rows = [
        vec![t(), i(1)],
        vec![f(), i(2)],
        vec![t(), i(3)],
        vec![f(), i(4)],
    ];
    let (eager, lazy) = eager_and_lazy(source, &rows);
    assert_eq!(eager, [Value::Deferred, i(2), i(4), i(6)]);
    // `x[1]` is the stream's previous tick; `(x + 0)[1]` the branch's own.
    assert_eq!(lazy, [Value::Deferred, Value::Deferred, i(3), i(5)]);
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
        run(&format!("{LAZY}{COMPUTED_THEN}"), &rows),
        [Value::NoVal, Value::Deferred, Value::Deferred, i(2), i(4)]
    );
    // The stream's own history saw every tick, selected or not.
    let stream = [Value::NoVal, i(1), Value::Deferred, i(3), i(4)];
    assert_eq!(run(&format!("{LAZY}{DELAYED_THEN}"), &rows), stream);
    assert_eq!(run(DELAYED_THEN, &rows), stream);
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
    let header = "in a: Bool\nin b: Bool\nin x: Int\nout y: Int\n";
    let rows = [
        vec![t(), f(), i(1)],
        vec![f(), t(), i(2)],
        vec![t(), t(), i(3)],
        vec![t(), t(), i(4)],
    ];
    let computed = format!("{header}y = if a then (if b then (x + 0)[1] else 0) else 0\n");
    let (eager, lazy) = eager_and_lazy(&computed, &rows);
    assert_eq!(eager, [i(0), i(0), i(2), i(3)]);
    // The inner then-branch runs first at tick 2.
    assert_eq!(lazy, [i(0), i(0), Value::Deferred, i(3)]);
    // A stream's previous value is the same however deeply it is nested.
    let stream = format!("{header}y = if a then (if b then x[1] else 0) else 0\n");
    let (eager, lazy) = eager_and_lazy(&stream, &rows);
    assert_eq!(eager, [i(0), i(0), i(2), i(3)]);
    assert_eq!(lazy, eager);
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

/// Text supplied at run time has no equation to read a previous value
/// outside the `if` in, so a lazy branch there refuses a stream's previous
/// value rather than giving it the branch's timeline.
#[test]
fn runtime_text_refuses_a_previous_value_in_a_lazy_branch() {
    let header = "in c: Bool\nin x: Int\nin s: Str\nout z: Int\nz = dynamic(s: Int)\n";
    let text = |text: &str| Value::Str(text.into());
    for semantics in [Semantics::Untimed, Semantics::TypedUntimed] {
        let mut lazy = DataflowMonitor::compile_with_semantics(
            elaborated(&format!("{LAZY}{header}")),
            semantics,
        )
        .unwrap();
        let mut output = [Value::NoVal];
        // Outside an `if` the text is accepted.
        lazy.evaluate(&[t(), i(1), text("default(x[1], 0)")], &mut output)
            .unwrap();
        assert_eq!(output, [i(0)]);
        let mut lazy = DataflowMonitor::compile_with_semantics(
            elaborated(&format!("{LAZY}{header}")),
            semantics,
        )
        .unwrap();
        let error = lazy
            .evaluate(
                &[t(), i(1), text("if c then default(x[1], 0) else 0")],
                &mut output,
            )
            .unwrap_err();
        assert!(
            matches!(
                &error,
                DataflowEvaluationError::InvalidDynamicProgram(
                    StreamProgramError::PreviousValueInRuntimeText { stream, offset: 1 }
                ) if stream.name() == "x"
            ),
            "{error}"
        );
        assert!(
            error.to_string().contains("in an equation outside"),
            "{error}"
        );
        // A `match` arm in runtime text refuses it too, `lazy_if` or not.
        let mut matching = DataflowMonitor::compile_with_semantics(
            elaborated(&format!("{MATCHING}{header}")),
            semantics,
        )
        .unwrap();
        let error = matching
            .evaluate(
                &[
                    t(),
                    i(1),
                    text("match(c) { true -> default(x[1], 0), _ -> 0 }"),
                ],
                &mut output,
            )
            .unwrap_err();
        assert!(
            matches!(
                &error,
                DataflowEvaluationError::InvalidDynamicProgram(
                    StreamProgramError::PreviousValueInRuntimeText { .. }
                )
            ),
            "{error}"
        );
        // An eager `if` in the same text is unaffected.
        let mut eager =
            DataflowMonitor::compile_with_semantics(elaborated(header), semantics).unwrap();
        eager
            .evaluate(
                &[t(), i(1), text("if c then default(x[1], 0) else 0")],
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [i(0)]);
    }
}

/// How each canonical way of compiling `source` refuses it, which must agree.
fn refusal(source: &str) -> StreamProgramError {
    let refuse =
        |semantics| match DataflowMonitor::compile_with_semantics(elaborated(source), semantics) {
            Err(DataflowCompilationError::InvalidStreamProgram(error)) => error,
            Err(error) => panic!("unexpected refusal {error}\n{source}"),
            Ok(_) => panic!("accepted\n{source}"),
        };
    let typed = refuse(Semantics::TypedUntimed);
    assert_eq!(refuse(Semantics::Untimed).to_string(), typed.to_string());
    typed
}

#[test]
fn a_previous_value_in_a_match_arm_or_guard_is_the_streams_previous_tick() {
    let header = format!("{MATCHING}in c: Bool\nin x: Int\nout y: Int\n");
    let rows = [
        vec![f(), i(1)],
        vec![f(), i(2)],
        vec![t(), i(3)],
        vec![f(), i(4)],
        vec![t(), i(5)],
    ];
    // An arm runs only when selected, with or without `lazy_if`.
    let arm = "y = match(c) { true -> default(x[1], 0), _ -> 0 }\n";
    assert_eq!(
        run(&format!("{header}{arm}"), &rows),
        [i(0), i(0), i(2), i(0), i(4)]
    );
    let own = "y = match(c) { true -> default(y[1], 0) + 1, _ -> 0 }\n";
    assert_eq!(
        run(&format!("{header}{own}"), &rows),
        [i(0), i(0), i(1), i(0), i(1)]
    );
    // A guard is tried only when no earlier arm is selected: at tick 3 it
    // compares with 9, which it never saw, not with 5.
    let guard =
        "y = match(Tuple(c, x)) { (true, _) -> 0, (_, v) if default(x[1], 0) < v -> 1, _ -> 2 }\n";
    let rows = [
        vec![f(), i(1)],
        vec![f(), i(5)],
        vec![t(), i(9)],
        vec![f(), i(6)],
    ];
    assert_eq!(
        run(&format!("{header}{guard}"), &rows),
        [i(1), i(1), i(0), i(2)]
    );
    assert_eq!(
        run(&format!("{LAZY}{header}{guard}"), &rows),
        [i(1), i(1), i(0), i(2)]
    );
}

#[test]
fn a_previous_value_in_a_lambda_in_a_lazy_branch_or_arm_is_the_streams() {
    let header = "in c: Bool\nin x: Int\nout y: Int\n";
    let rows = [vec![f(), i(1)], vec![f(), i(2)], vec![t(), i(3)]];
    let branch = "y = if c then (\\v: Int -> v + default(x[1], 0))(x) else 0\n";
    assert_eq!(
        run(&format!("{LAZY}{header}{branch}"), &rows),
        [i(0), i(0), i(5)]
    );
    let arm = "y = match(c) { true -> (\\v: Int -> v + default(x[1], 0))(x), _ -> 0 }\n";
    assert_eq!(
        run(&format!("{MATCHING}{header}{arm}"), &rows),
        [i(0), i(0), i(5)]
    );
}

#[test]
fn a_previous_value_of_a_bound_name_where_only_some_ticks_run_is_refused() {
    let header = "in c: Bool\nin x: Int\nout y: Int\n";
    let parameter = "y = if c then (\\v: Int -> default(v[1], 0))(x) else 0\n";
    let pattern = "y = match(Tuple(c, x)) { (true, v) -> default(v[1], 0), _ -> 0 }\n";
    for (source, name) in [
        (format!("{LAZY}{header}{parameter}"), "v"),
        (format!("{MATCHING}{header}{pattern}"), "v"),
    ] {
        let error = refusal(&source);
        assert!(
            matches!(
                &error,
                StreamProgramError::PreviousValueOfBoundName { name: bound, offset: 1 }
                    if bound.name() == name
            ),
            "{error}"
        );
        assert!(
            error.to_string().contains("lambda parameter or pattern"),
            "{error}"
        );
    }
    // An eager branch calls the lambda on every tick, so its parameter's
    // history is the argument's, as it is outside any `if`.
    let rows = [vec![f(), i(1)], vec![f(), i(2)], vec![t(), i(3)]];
    assert_eq!(
        run(&format!("{header}{parameter}"), &rows),
        [i(0), i(0), i(2)]
    );
}

/// A delay that stays in an arm or guard, of a computed expression, keeps
/// the ticks that run it, and its writes are committed like any other's.
#[test]
fn a_computed_history_in_a_match_arm_or_guard_advances_on_the_ticks_that_run_it() {
    let header = format!("{MATCHING}in c: Bool\nin x: Int\nout y: Int\n");
    let rows = [
        vec![t(), i(1)],
        vec![t(), i(2)],
        vec![f(), i(3)],
        vec![t(), i(4)],
    ];
    for arm in [
        "y = match(c) { true -> default((x + 0)[1], 0), _ -> 0 }\n",
        "y = match(c) { true -> default((\\v: Int -> (v + 0)[1])(x), 0), _ -> 0 }\n",
    ] {
        // The arm saw 1, 2 and 4, so at tick 3 its previous value is 2.
        assert_eq!(
            run(&format!("{header}{arm}"), &rows),
            [i(0), i(1), i(0), i(2)]
        );
    }
    // The guard is tried at ticks 0, 1 and 3, so at tick 3 it compares with 5.
    let guard = "y = match(Tuple(c, x)) { (true, _) -> 0, (_, v) if default((x + 0)[1], 0) < v -> 1, _ -> 2 }\n";
    let rows = [
        vec![f(), i(1)],
        vec![f(), i(5)],
        vec![t(), i(9)],
        vec![f(), i(6)],
    ];
    assert_eq!(
        run(&format!("{header}{guard}"), &rows),
        [i(1), i(1), i(0), i(1)]
    );
}
