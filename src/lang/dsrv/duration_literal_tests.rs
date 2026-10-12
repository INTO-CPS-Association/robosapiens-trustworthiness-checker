//! Typed unit suffixes share the std::time factory and its nominal domains.
use crate::Value;
use crate::dataflow::DataflowMonitor;
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::{TypeCheckOptions, parser::parse_str};

const HEADER: &str = "use experimental::{modules, functions, generics, newtypes, duration_literals, constants, tagged_unions, pattern_matching}\nuse std::time::{Duration, Instant, TimeInterval}\nuse std::option::{Option, Some, None}\nnewtype Clock = Unit\n";

fn checks(body: &str) -> bool {
    parse_str(&format!("{HEADER}{body}")).is_ok_and(|spec| {
        spec.check(TypeCheckOptions::STRICT)
            .without_warnings()
            .is_ok()
    })
}

fn compile(body: &str) -> DataflowMonitor {
    let source = format!("{HEADER}{body}");
    let checked = parse_str(&source)
        .unwrap_or_else(|error| panic!("{source}: {error}"))
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    DataflowMonitor::compile_checked(checked).unwrap()
}

#[test]
fn duration_literals_normalise_all_units_once_before_float_rounding() {
    for (literal, seconds) in [
        ("100 ms", "0.1 s"),
        ("100ms", "0.1s"),
        ("100000000 ns", "0.1 s"),
        ("100000 us", "0.1 s"),
        ("100000 µs", "0.1 s"),
        ("100000 μs", "0.1 s"),
        ("1 min", "60 s"),
        ("1 h", "3600 s"),
        ("1 d", "86400 s"),
        ("0.03 ms", "0.00003 s"),
        ("0.03 min", "1.8 s"),
        ("0.07 h", "252 s"),
        ("0.07 d", "6048 s"),
        ("3e-2 ms", "3e-5 s"),
        ("1\tms", "0.001 s"),
        ("-1 ms", "-0.001 s"),
    ] {
        let mut monitor = compile(&format!(
            "const first: Duration<Clock> = {literal}\nconst second: Duration<Clock> = {seconds}\nout same: Bool = first == second\n"
        ));
        let mut row = vec![Value::NoVal];
        monitor.evaluate(&[], &mut row).unwrap();
        assert_eq!(row, [Value::Bool(true)], "{literal} != {seconds}");
    }
}

#[test]
fn duration_literals_keep_strict_thresholds_at_adjacent_float_values() {
    for (literal, seconds) in [
        ("100 ms", 0.1_f64),
        ("0.03 ms", 0.00003_f64),
        ("0.03 min", 1.8_f64),
    ] {
        let mut monitor = compile(&format!(
            "in now: Float\naux start: Instant<Clock> = std::time::instant(0.0)\naux end: Instant<Clock> = std::time::instant(now)\naux span: Option<TimeInterval<Clock>> = Some(std::time::between(start, end))\nout late: Bool = std::time::exceeds(span, {literal})\nout shorter: Bool = std::time::shorter_than(span, {literal})\n"
        ));
        let outputs = monitor.output_vars().to_vec();
        for (now, late, shorter) in [
            (f64::from_bits(seconds.to_bits() - 1), false, true),
            (seconds, false, false),
            (f64::from_bits(seconds.to_bits() + 1), true, false),
            (-1.0, false, false),
        ] {
            let mut row = vec![Value::NoVal; outputs.len()];
            monitor.evaluate(&[Value::Float(now)], &mut row).unwrap();
            for (name, actual) in outputs.iter().zip(row) {
                let expected = if name.name() == "late" { late } else { shorter };
                assert_eq!(
                    actual,
                    Value::Bool(expected),
                    "{literal}, now={now:?}, {name}"
                );
            }
        }
    }
}

#[test]
fn duration_literals_receive_domains_from_constants_results_and_siblings() {
    assert!(checks(
        "const budget: Duration<Clock> = 100 ms\nout d: Duration<Clock> = 0.1 s\n"
    ));
    for call in ["std::time::exceeds(span, 100 ms)", "reverse(100 ms, span)"] {
        assert!(
            checks(&format!(
                "def reverse<D>(limit: Duration<D>, span: Option<TimeInterval<D>>) -> Bool = std::time::exceeds(span, limit)\nin span: Option<TimeInterval<Clock>>\nout late: Bool = {call}\n"
            )),
            "{call}"
        );
    }
    assert!(checks(
        "def identity<T>(value: T) -> T = value\nout d: Duration<Clock> = identity(100 ms)\n"
    ));
    assert!(checks(
        "def limit<D>() -> Duration<D> = 100 ms\nout d: Duration<Clock> = limit()\n"
    ));
    for comparison in ["budget == 0.1 s", "0.1 s == budget", "budget != 0.2 s"] {
        assert!(checks(&format!(
            "const budget: Duration<Clock> = 100 ms\nout same: Bool = {comparison}\n"
        )));
    }
}

#[test]
fn duration_literals_reject_erasure_missing_domains_and_cross_clock_calls() {
    for body in [
        "out d: Float = 100 ms\n",
        "out d = 100 ms\n",
        "newtype Other = Unit\nin span: Option<TimeInterval<Clock>>\nconst limit: Duration<Other> = 100 ms\nout late: Bool = std::time::exceeds(span, limit)\n",
        "in span: Option<TimeInterval<Clock>>\nout late: Bool = std::time::exceeds_seconds(span, 100 ms)\n",
        "out d: Duration<Clock> = 1 ms + 2 ms\n",
    ] {
        assert!(!checks(body), "accepted {body}");
    }
}

#[test]
fn duration_literals_load_the_factory_without_reserving_or_shadowing_names() {
    let mut monitor = compile(
        "in s: Int\nin ms: Int\nin min: Int\ndef seconds(value: Float) -> Bool = false\naux first: Duration<Clock> = 100 ms\naux second: Duration<Clock> = std::time::seconds(0.1)\nout same: Bool = first == second\n",
    );
    let mut row = [Value::NoVal];
    monitor
        .evaluate(&[Value::Int(1), Value::Int(2), Value::Int(3)], &mut row)
        .unwrap();
    assert_eq!(row, [Value::Bool(true)]);
}

#[test]
fn duration_literals_are_gated_and_invalid_suffixes_and_ranges_are_rejected() {
    let disabled = HEADER.replace(", duration_literals", "");
    let error = parse_str(&format!("{disabled}out d: Duration<Clock> = 100 ms\n")).unwrap_err();
    assert!(error.to_string().contains("duration_literals"), "{error}");
    for literal in [
        "1 fortnight",
        "1 msomething",
        "1.2.3 ms",
        "1e309 s",
        "1e308 d",
        "1\nms",
    ] {
        assert!(
            parse_str(&format!("{HEADER}out d: Duration<Clock> = {literal}\n")).is_err(),
            "accepted {literal}"
        );
    }
}

#[test]
fn duration_literals_print_canonical_seconds_and_reparse() {
    let spec = parse_str(&format!("{HEADER}out d: Duration<Clock> = -100 ms\n")).unwrap();
    let printed = spec.var_expr_ref(&"d".into()).unwrap().to_string();
    assert_eq!(printed, "-0.1 s");
    let reparsed = parse_str(&format!("{HEADER}out d: Duration<Clock> = {printed}\n")).unwrap();
    reparsed
        .clone()
        .check(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    assert_eq!(
        reparsed.var_expr_ref(&"d".into()).unwrap().to_string(),
        printed
    );
}

#[test]
fn imported_modules_keep_literal_settings_and_constant_clock_domains() {
    use crate::lang::dsrv::modules::ModuleCollector;
    let mut collector = ModuleCollector::new(
        "use experimental::{modules, functions, generics, newtypes}\nmod budgets\nuse budgets\nout d: budgets::Limit = budgets::limit\nout same: Bool = budgets::equal(budgets::limit)\n"
    ).unwrap();
    collector.supply(
        "use experimental::{modules, functions, generics, newtypes, duration_literals, constants}\nuse std::time::Duration\nnewtype Clock = Unit\ntype Limit = Duration<Clock>\nconst limit: Limit = 100 ms\ndef equal(value: Limit) -> Bool = value == 0.1 s\n"
    ).unwrap();
    let spec =
        crate::lang::dsrv::expand::expand_program(collector.finish().unwrap(), Default::default())
            .unwrap();
    let checked = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(checked).unwrap();
    let outputs = monitor.output_vars().to_vec();
    let mut row = vec![Value::NoVal; outputs.len()];
    monitor.evaluate(&[], &mut row).unwrap();
    assert_eq!(
        row[outputs
            .iter()
            .position(|name| name.name() == "same")
            .unwrap()],
        Value::Bool(true)
    );
}

#[test]
fn runtime_duration_literals_use_the_sites_existing_library_and_domain_context() {
    use crate::lang::dsrv::runtime_expression::is_runtime_expression;
    use contiguous_tree::TreeCursorExt;
    let checked = parse_str(&format!("{HEADER}use std::time\nin text: Str\nout d: Duration<Clock> = dynamic(text: Duration<Clock>)\n"))
        .unwrap().check_and_elaborate(TypeCheckOptions::STRICT).without_warnings().unwrap();
    let site = checked
        .var_expr_ref(&"d".into())
        .unwrap()
        .postorder()
        .find(|node| is_runtime_expression(node.expr()))
        .unwrap()
        .runtime_expression()
        .clone();
    assert!(site.parse_and_check("100 ms").is_ok());
    assert!(site.parse_and_check("-100 ms").is_ok());
    assert!(
        site.parse_and_check("std::time::Duration::new(0.1)")
            .is_err()
    );
}
