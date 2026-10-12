//! Equation-local aux streams compared with explicit flat equations.
use crate::dataflow::DataflowMonitor;
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::TypeCheckOptions;
use crate::lang::dsrv::ast::DsrvSpecification;
use crate::lang::dsrv::expand::DsrvExpandError;
use crate::lang::dsrv::modules::{ModuleCollector, show_path};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::source_map::SourceLabel;
use crate::{Value, VarName};
use std::collections::BTreeMap;

const BOTH: [TypeCheckOptions; 2] = [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL];
const HEADER: &str = "use experimental::high_level_dsrv\n";

fn expand_with(root: &str, modules: &[(&str, &str)]) -> Result<DsrvSpecification, DsrvExpandError> {
    let label = |name: &str| SourceLabel::Path(format!("{name}.dsrv").into());
    let mut collector = ModuleCollector::with_label(root, label("root")).expect("a parsable root");
    while let Some(module) = collector.next_request().map(<[ModuleName]>::to_vec) {
        let wanted = show_path(&module);
        let text = modules
            .iter()
            .find(|(name, _)| *name == wanted)
            .unwrap_or_else(|| panic!("no source for {wanted}"))
            .1;
        collector
            .supply_labelled(text, label(&wanted))
            .expect("a parsable module");
    }
    crate::lang::dsrv::expand::expand_program(collector.finish().expect("collected"), None)
}

fn expand(body: &str) -> Result<DsrvSpecification, DsrvExpandError> {
    expand_with(&format!("{HEADER}{body}"), &[])
}
fn program(body: &str) -> DsrvSpecification {
    expand(body).unwrap_or_else(|error| panic!("{body}: {error}"))
}
fn run(spec: &DsrvSpecification, rows: &[&[(&str, Value)]]) -> Vec<BTreeMap<String, Value>> {
    let mut agreed: Option<Vec<BTreeMap<String, Value>>> = None;
    for options in BOTH {
        let elaborated = spec
            .clone()
            .check_and_elaborate(options)
            .without_warnings()
            .unwrap_or_else(|errors| panic!("{:?}: {errors:?}\n{spec}", options.mode));
        let monitors = [
            DataflowMonitor::compile_checked(elaborated.clone()).unwrap(),
            DataflowMonitor::compile_with_semantics(elaborated, crate::core::Semantics::Untimed)
                .unwrap(),
        ];
        for mut monitor in monitors {
            let inputs = monitor.input_vars().to_vec();
            let outputs = monitor.output_vars().to_vec();
            let results = rows
                .iter()
                .map(|row| {
                    let input: Vec<Value> = inputs
                        .iter()
                        .map(|name| {
                            row.iter()
                                .find(|(written, _)| VarName::from(*written) == *name)
                                .map_or(Value::NoVal, |(_, value)| value.clone())
                        })
                        .collect();
                    let mut output = vec![Value::NoVal; outputs.len()];
                    monitor.evaluate(&input, &mut output).unwrap();
                    outputs
                        .iter()
                        .map(|name| name.name())
                        .zip(output)
                        .collect::<BTreeMap<_, _>>()
                })
                .collect::<Vec<_>>();
            match &agreed {
                Some(agreed) => assert_eq!(&results, agreed, "{:?}", options.mode),
                None => agreed = Some(results),
            }
        }
    }
    agreed.expect("at least one monitor")
}

/// One output over every row.
fn column(results: &[BTreeMap<String, Value>], name: &str) -> Vec<Value> {
    results.iter().map(|row| row[name].clone()).collect()
}

#[test]
fn combined_and_split_equation_locals_match_flat_feedback() {
    let rows = [
        &[("x", Value::Int(2))][..],
        &[("x", Value::Int(-1))][..],
        &[("x", Value::Int(3))][..],
    ];
    let flat = program("in x: Int\naux total: Int = default(total[1], 0) + x\nout y: Int = total");
    for source in [
        "in x: Int\nout y: Int = total where { total: Int = default(total[1], 0) + x }",
        "in x: Int\nout y: Int = total where { total: Int\ntotal = previous + x where { previous: Int = default(total[1], 0) } }",
        "in x: Int\nout y: Int = total where { aux total: Int = default(total[1], 0) + x }",
        "in x: Int\naux intermediate: Int\nintermediate = total\nwhere { aux total: Int\ntotal = default(total[1], 0) + x }\nout y: Int = intermediate",
    ] {
        let result = run(&program(source), &rows);
        assert_eq!(column(&result, "y"), column(&run(&flat, &rows), "y"));
        assert_eq!(
            column(&result, "y"),
            vec![Value::Int(2), Value::Int(1), Value::Int(4)]
        );
    }
}

#[test]
fn empty_equation_where_blocks_leave_values_unchanged() {
    let spec = program(
        "in x: Int\naux intermediate: Int = x + 1 where {}\nout y: Int = intermediate\nwhere {}",
    );
    assert_eq!(
        column(&run(&spec, &[&[("x", Value::Int(4))]]), "y"),
        vec![Value::Int(5)]
    );
}

#[test]
fn local_names_shadow_outer_streams_without_leaking_to_siblings() {
    let spec = program(
        "in x: Int\naux next: Int = 100\nout y: Int = next where { aux next: Int = later\naux later: Int = x + 1 }\nout sibling: Int = next",
    );
    let results = run(&spec, &[&[("x", Value::Int(4))]]);
    assert_eq!(column(&results, "y"), vec![Value::Int(5)]);
    assert_eq!(column(&results, "sibling"), vec![Value::Int(100)]);
    let leaky =
        program("out y: Int = secret where { aux secret: Int = 1 }\nout sibling: Int = secret");
    assert!(
        leaky
            .check_and_elaborate(TypeCheckOptions::STRICT)
            .result()
            .is_err()
    );
}

#[test]
fn owner_history_matches_an_ordinary_delayed_equation() {
    let local = program(
        "in x: Int\nout y: Int = increment where { aux increment: Int = default(y[1], 0) + x }",
    );
    let flat = program("in x: Int\nout y: Int = default(y[1], 0) + x");
    let rows = [
        &[("x", Value::Int(2))][..],
        &[("x", Value::Int(-1))][..],
        &[("x", Value::Int(3))][..],
    ];
    assert_eq!(run(&local, &rows), run(&flat, &rows));
}

#[test]
fn conditional_results_do_not_give_locals_a_branch_clock() {
    let spec = program(
        "in hit: Bool\nout y: Int = if hit then ticks else 0 where { aux ticks: Int = default(ticks[1], 0) + 1 }",
    );
    let flat = program(
        "in hit: Bool\naux ticks: Int = default(ticks[1], 0) + 1\nout y: Int = if hit then ticks else 0",
    );
    let rows = [
        &[("hit", Value::Bool(true))][..],
        &[("hit", Value::Bool(false))][..],
        &[("hit", Value::Bool(true))][..],
    ];
    let results = run(&spec, &rows);
    assert_eq!(results, run(&flat, &rows));
    assert_eq!(
        column(&results, "y"),
        vec![Value::Int(1), Value::Int(0), Value::Int(3)]
    );
}

#[test]
fn nested_equation_blocks_keep_independent_def_instances() {
    let spec = program(
        "def total(x: Int) -> Int = sum where { aux sum: Int = default(sum[1], 0) + x }\nin x: Int\nout a: Int = child where { aux child: Int = counted where { aux counted: Int = total(x) } }\nout b: Int = child where { aux child: Int = total(x + x) }\nout shared: Int = a + a",
    );
    let results = run(
        &spec,
        &[
            &[("x", Value::Int(2))],
            &[("x", Value::Int(-1))],
            &[("x", Value::Int(3))],
        ],
    );
    assert_eq!(
        column(&results, "a"),
        vec![Value::Int(2), Value::Int(1), Value::Int(4)]
    );
    assert_eq!(
        column(&results, "b"),
        vec![Value::Int(4), Value::Int(2), Value::Int(8)]
    );
    assert_eq!(column(&results, "shared"), column(&results, "b"));
}

#[test]
fn equation_where_rejects_duplicate_locals_and_owner_name_collisions() {
    for source in [
        "out y: Int = local where { aux local: Int = 1\naux local: Int = 2 }",
        "out y: Int = 1 where { aux y: Int = 2 }",
    ] {
        assert!(
            expand(source).is_err(),
            "accepted ambiguous scope: {source}"
        );
    }
}

#[test]
fn equation_where_requires_the_local_streams_experiment() {
    use crate::lang::dsrv::expand::language::LanguageError;
    let error = expand_with("out y: Int = 1 where {}", &[]).expect_err("feature not enabled");
    assert!(
        matches!(
            error,
            DsrvExpandError::Language(LanguageError::NeedsExperiment {
                feature: "local_streams",
                ..
            })
        ),
        "{error:?}"
    );
}

#[test]
fn pure_equation_locals_do_not_require_stream_function_runtime_admission() {
    use crate::core::ensure_no_stream_function_instances;
    let spec = program("out y: Int = count where { aux count: Int = default(count[1], 0) + 1 }");
    ensure_no_stream_function_instances(&spec, "reconf-dataflow").unwrap();
    let instantiated = program(
        "def next(x: Int) -> Int = x where {}\nout y: Int = local where { aux local: Int = next(1) }",
    );
    assert!(ensure_no_stream_function_instances(&instantiated, "reconf-dataflow").is_err());
}

#[test]
fn absent_ticks_preserve_flat_input_history_and_advance_local_state() {
    let source = "in x: Int\nout previous: Int = default(sample[1], -1) where { aux sample: Int = x }\nout ticks: Int = count where { aux count: Int = default(count[1], 0) + 1 }";
    let flat = "in x: Int\naux sample: Int = x\naux count: Int = default(count[1], 0) + 1\nout previous: Int = default(sample[1], -1)\nout ticks: Int = count";
    let rows = [
        &[("x", Value::Int(2))][..],
        &[("x", Value::NoVal)][..],
        &[("x", Value::Int(3))][..],
    ];
    let results = run(&program(source), &rows);
    assert_eq!(results, run(&program(flat), &rows));
    assert_eq!(
        column(&results, "previous"),
        vec![Value::Int(-1), Value::Int(2), Value::Int(2)]
    );
    assert_eq!(
        column(&results, "ticks"),
        vec![Value::Int(1), Value::Int(2), Value::Int(3)]
    );
}

#[test]
fn zero_delay_cycles_through_equation_locals_are_refused() {
    if let Ok(spec) = expand("out y: Int = local where { aux local: Int = y }") {
        if let Ok(checked) = spec
            .check_and_elaborate(TypeCheckOptions::STRICT)
            .without_warnings()
        {
            assert!(
                DataflowMonitor::compile_checked(checked).is_err(),
                "local scope hid a zero-delay cycle"
            );
        }
    }
}

#[test]
fn equation_blocks_refuse_input_output_and_def_declarations() {
    for declaration in [
        "in local: Int",
        "out local: Int = 1",
        "def helper(x: Int) -> Int = x",
    ] {
        let source = format!("{HEADER}out y: Int = 1 where {{ {declaration} }}");
        assert!(
            ModuleCollector::with_label(&source, SourceLabel::Path("invalid.dsrv".into())).is_err(),
            "accepted local {declaration}"
        );
    }
}

#[test]
fn local_equations_keep_stateful_call_placement_restrictions() {
    let source = "def total(x: Int) -> Int = sum where { aux sum: Int = default(sum[1], 0) + x }\nin x: Int\nout y: Int = local where { aux local: Int = total(x) + 1 }";
    assert!(
        expand(source).is_err(),
        "an equation block hid a stateful call in a larger expression"
    );
}

#[test]
fn local_type_errors_retain_the_original_source_expression() {
    let spec = program("out y: Int = local where { aux local: Int = false }");
    let errors = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .expect_err("local expression has the wrong type");
    assert!(
        errors.iter().any(|error| {
            [error.location().primary(), error.location().definition()]
                .into_iter()
                .flatten()
                .any(|site| {
                    site.snippet() == Some("false")
                        && site.label() == &SourceLabel::Path("root.dsrv".into())
                })
        }),
        "local error lost its original expression location: {errors:?}"
    );
}

#[test]
fn implicit_local_equations_keep_ordinary_gradual_typing() {
    let spec = program("out y: Int = local where { local = 5 }");
    let checked = spec
        .check_and_elaborate(TypeCheckOptions::GRADUAL)
        .without_warnings()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(checked).unwrap();
    let mut output = [Value::NoVal];
    monitor.evaluate(&[], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);
}

#[test]
fn lambda_and_pattern_binders_shadow_equation_local_names() {
    let spec = program(
        "out y: Int = List.fold(\\acc: Int, x: Int -> acc + x, 0, [1, 2]) + match(4) { x -> x } + x where { aux x: Int = 100 }",
    );
    assert_eq!(column(&run(&spec, &[&[]]), "y"), vec![Value::Int(107)]);
}

#[test]
fn function_valued_equation_locals_remain_callable_values() {
    let spec = program(
        "out y: Int = selected(4) where { aux selected: (Int -> Int) = \\n: Int -> n + 1 }",
    );
    assert_eq!(column(&run(&spec, &[&[]]), "y"), vec![Value::Int(5)]);
}

#[test]
fn nested_equation_locals_inside_defs_keep_private_feedback() {
    let spec = program(
        "def total(x: Int) -> Int = local where { aux local: Int = sum where { aux sum: Int = default(sum[1], 0) + x } }\nin x: Int\nout a: Int = total(x)\nout b: Int = total(x + x)",
    );
    let results = run(
        &spec,
        &[
            &[("x", Value::Int(2))],
            &[("x", Value::Int(-1))],
            &[("x", Value::Int(3))],
        ],
    );
    assert_eq!(
        column(&results, "a"),
        vec![Value::Int(2), Value::Int(1), Value::Int(4)]
    );
    assert_eq!(
        column(&results, "b"),
        vec![Value::Int(4), Value::Int(2), Value::Int(8)]
    );
}
