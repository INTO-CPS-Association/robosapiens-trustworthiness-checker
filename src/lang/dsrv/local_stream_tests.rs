//! Local stream defs: histories, syntax boundaries and transitive placement.
use crate::dataflow::DataflowMonitor;
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::TypeCheckOptions;
use crate::lang::dsrv::ast::DsrvSpecification;
use crate::lang::dsrv::expand::DsrvExpandError;
use crate::lang::dsrv::modules::{ModuleCollector, show_path};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::source_map::SourceLabel;
use crate::{Value, VarName};
use contiguous_tree::TreeCursorExt;
use std::collections::BTreeMap;

const BOTH: [TypeCheckOptions; 2] = [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL];
const HEADER: &str = "use experimental::high_level_dsrv\n";
const COUNTER: &str = "def sum(value: Int) -> Int = total where {\ntotal: Int\ntotal = default(total[1], 0) + value\n}\n";
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
fn local_split_equations_keep_independent_history_and_shared_results() {
    let spec = program(&format!(
        "{COUNTER}
in x: Int
aux first: Int = sum(x)
aux second: Int = sum(x + x)
out a: Int = first
out b: Int = second
out shared: Int = first + first"
    ));
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
fn forwarding_defs_create_one_child_instance_per_written_call() {
    let spec = program(&format!(
        "{COUNTER}
def forwarded(x: Int) -> Int = sum(x)
def nested(x: Int) -> Int = forwarded(x) where {{}}
in x: Int
out a: Int = forwarded(x)
out b: Int = nested(x + 1)"
    ));
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
        vec![Value::Int(3), Value::Int(3), Value::Int(7)]
    );
}

#[test]
fn an_empty_where_returns_the_declared_result() {
    let spec =
        program("def identity(x: Int) -> Int = x where {}\nin x: Int\nout y: Int = identity(x)");
    let results = run(&spec, &[&[("x", Value::Int(8))], &[("x", Value::Int(-2))]]);
    assert_eq!(column(&results, "y"), vec![Value::Int(8), Value::Int(-2)]);
}

#[test]
fn forwarding_cannot_hide_a_stream_call_inside_an_expression() {
    for expression in [
        "forwarded(x) + 1",
        "if true then forwarded(x) else 0",
        "match(true) { true -> forwarded(x), false -> 0 }",
        "(\\n: Int -> forwarded(n))(x)",
        "List.head(List.map(forwarded, [x]))",
    ] {
        let source = format!(
            "{COUNTER}def forwarded(x: Int) -> Int = sum(x)\nin x: Int\nout y: Int = {expression}"
        );
        assert!(
            expand(&source).is_err(),
            "stateful call admitted in {expression}"
        );
    }
}

#[test]
fn unused_local_stream_def_requires_its_defining_modules_experiment() {
    let source =
        "use experimental::functions\ndef identity(x: Int) -> Int = x where {}\nout y: Int = 1";
    assert!(
        expand_with(source, &[]).is_err(),
        "an unused where block bypassed feature checking"
    );
}

#[test]
fn fixed_named_selectors_work_with_inferred_type_parameters() {
    let spec = program(
        "def positive(x: Int) -> Bool = x > 0
def remembered<T, selected: (T -> Bool)>(input: T) -> Bool = seen
where {
 aux seen: Bool = default(seen[1], false) || selected(input)
}
in x: Int
out seen: Bool = remembered::<positive>(x)",
    );
    let results = run(
        &spec,
        &[
            &[("x", Value::Int(-1))],
            &[("x", Value::Int(2))],
            &[("x", Value::Int(-3))],
        ],
    );
    assert_eq!(
        column(&results, "seen"),
        vec![Value::Bool(false), Value::Bool(true), Value::Bool(true)]
    );
}

#[test]
fn local_blocks_refuse_inputs_outputs_and_nested_defs() {
    for declaration in [
        "in local: Int",
        "out local: Int = x",
        "def helper(n: Int) -> Int = n",
    ] {
        let source = format!(
            "{HEADER}def invalid(x: Int) -> Int = x where {{ {declaration} }}\nout y: Int = 1"
        );
        assert!(
            ModuleCollector::with_label(&source, SourceLabel::Path("invalid.dsrv".into())).is_err(),
            "accepted local {declaration}"
        );
    }
}

#[test]
fn stateful_defs_cannot_be_fixed_callbacks_even_through_forwarding() {
    for selector in ["stateful", "forwarded"] {
        let source = format!(
            "def stateful(x: Int) -> Bool = x > 0 where {{}}
def forwarded(x: Int) -> Bool = stateful(x)
def selected<check: (Int -> Bool)>(x: Int) -> Bool = check(x) where {{}}
in x: Int
out y: Bool = selected::<{selector}>(x)"
        );
        assert!(
            expand(&source).is_err(),
            "accepted stateful fixed callback {selector}"
        );
    }
}

#[test]
fn a_where_result_can_refer_to_a_later_local_equation() {
    let spec = program(
        "def next(x: Int) -> Int = later where {
 aux earlier: Int = later - 1
 aux later: Int = x + 1
}
in x: Int
out y: Int = next(x)",
    );
    let results = run(&spec, &[&[("x", Value::Int(4))]]);
    assert_eq!(column(&results, "y"), vec![Value::Int(5)]);
}

#[test]
fn an_empty_where_instance_is_refused_at_reconfiguration_admission() {
    use crate::core::ensure_no_stream_function_instances;
    let declaration = "def identity(x: Int) -> Int = x where {}\nin x: Int\n";
    let unused = program(&format!("{declaration}out y: Int = x"));
    ensure_no_stream_function_instances(&unused, "reconf-dataflow").unwrap();
    let used = program(&format!("{declaration}out y: Int = identity(x)"));
    assert!(ensure_no_stream_function_instances(&used, "reconf-dataflow").is_err());
}

#[test]
fn parameters_and_locals_named_like_functions_keep_their_lexical_binding() {
    let spec = program(
        "def selected(x: Int) -> Int = x + 100
def count(selected: Int) -> Int = selected where {
 aux count: Int = selected + 1
}
def next(x: Int) -> Int = selected where {
 aux selected: Int = x + 1
}
in x: Int
out a: Int = count(x)
out b: Int = next(x)",
    );
    let results = run(&spec, &[&[("x", Value::Int(4))], &[("x", Value::Int(6))]]);
    assert_eq!(column(&results, "a"), vec![Value::Int(4), Value::Int(6)]);
    assert_eq!(column(&results, "b"), vec![Value::Int(5), Value::Int(7)]);
}

#[test]
fn retired_template_syntax_and_experiment_are_rejected_separately() {
    assert!(
        ModuleCollector::with_label(
            "template count(x: Int) -> Int { out y = x }",
            SourceLabel::Path("legacy.dsrv".into())
        )
        .is_err()
    );
    assert!(expand_with("use experimental::templates\nout y: Int = 1", &[]).is_err());
}

#[test]
fn a_pure_local_def_shadows_an_imported_stream_function() {
    let library = format!("{HEADER}{COUNTER}");
    let root = format!(
        "{HEADER}mod library\nuse library::*\ndef sum(x: Int) -> Int = x + 1\nin x: Int\nout y: Int = sum(x) + 1"
    );
    let spec = expand_with(&root, &[("library", &library)]).unwrap();
    assert!(spec.stream_function_instances().is_empty());
    let results = run(&spec, &[&[("x", Value::Int(4))]]);
    assert_eq!(column(&results, "y"), vec![Value::Int(6)]);
}

#[test]
fn a_local_parameter_and_stream_shadow_imported_stateful_names() {
    let library = format!("{HEADER}{COUNTER}");
    let root = format!(
        "{HEADER}mod library\nuse library::*\ndef identity(sum: Int) -> Int = sum where {{}}\ndef next(x: Int) -> Int = sum where {{ aux sum: Int = x + 1 }}\nin x: Int\nout a: Int = identity(x)\nout b: Int = next(x)"
    );
    let spec = expand_with(&root, &[("library", &library)]).unwrap();
    let results = run(&spec, &[&[("x", Value::Int(4))]]);
    assert_eq!(column(&results, "a"), vec![Value::Int(4)]);
    assert_eq!(column(&results, "b"), vec![Value::Int(5)]);
}

#[test]
fn absent_argument_ticks_occupy_the_enclosing_history_clock() {
    let spec = program(
        "def previous(value: Int) -> Int = default(value[1], -1) where {}\ndef ticks(value: Int) -> Int = count where { aux count: Int = default(count[1], 0) + 1 }\nin x: Int\nout y: Int = previous(x)\nout reference: Int = default(x[1], -1)\nout clock: Int = ticks(x)",
    );
    let results = run(
        &spec,
        &[
            &[("x", Value::Int(2))],
            &[("x", Value::NoVal)],
            &[("x", Value::Int(3))],
        ],
    );
    assert_eq!(
        column(&results, "y"),
        vec![Value::Int(-1), Value::Int(2), Value::Int(2)]
    );
    // Ordinary lifted input semantics retain the last value on NoVal, as
    // pinned by toggling_quickening_preserves_lift_state.
    assert_eq!(column(&results, "y"), column(&results, "reference"));
    assert_eq!(
        column(&results, "clock"),
        vec![Value::Int(1), Value::Int(2), Value::Int(3)]
    );
}

#[test]
fn forwarding_cannot_supply_a_constant() {
    let source = format!(
        "{COUNTER}def forwarded(x: Int) -> Int = sum(x)\nconst y: Int = forwarded(1)\nout result: Int = y"
    );
    assert!(
        expand(&source).is_err(),
        "stateful forwarding reached constant evaluation"
    );
}

#[test]
fn runtime_dynamic_and_defer_text_cannot_instantiate_a_forwarding_def() {
    use crate::lang::dsrv::ast::ExprKind;
    for operation in ["dynamic", "defer"] {
        let spec = program(&format!("{COUNTER}def forwarded(x: Int) -> Int = sum(x)\nin source: Str\nin x: Int\nout y: Int = {operation}(source: Int)"))
            .check_and_elaborate(TypeCheckOptions::STRICT)
            .without_warnings()
            .unwrap();
        let expression = spec.var_expr(&VarName::from("y")).unwrap();
        let site = expression
            .as_ref()
            .postorder()
            .find(|node| matches!(node.kind(), ExprKind::Dynamic(..) | ExprKind::Defer(..)))
            .unwrap()
            .runtime_expression()
            .clone();
        assert!(
            site.parse_and_check_for("x + 1", crate::dataflow::RUNTIME_CAPABILITIES, "dataflow")
                .is_ok()
        );
        assert!(
            site.parse_and_check_for(
                "forwarded(x)",
                crate::dataflow::RUNTIME_CAPABILITIES,
                "dataflow"
            )
            .is_err(),
            "{operation} text admitted a transitive stream function"
        );
    }
}

#[test]
fn unused_forwarding_defs_require_local_streams_in_their_defining_module() {
    use crate::lang::dsrv::expand::language::LanguageError;
    let library = "use experimental::{functions, local_streams}\ndef f(x: Int) -> Int = x where {}";
    let root = "use experimental::{modules, functions}\nmod library\nuse library\ndef forwarded(x: Int) -> Int = library::f(x)\nout y: Int = 1";
    let error = expand_with(root, &[("library", library)])
        .expect_err("an unused forwarding def still introduces local streams");
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
