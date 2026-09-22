//! Code inlined from another module keeps the environment it was written in.
//!
//! A def's body is judged by the header of the module that wrote it and
//! resolves names in that module's namespace, wherever it is called from;
//! the arguments a caller writes are the caller's. Text supplied at run time
//! to a `dynamic` a library wrote is read where the library wrote it. Which
//! runtimes may admit any of it is still the program's to decide.

use contiguous_tree::TreeCursorExt;

use crate::Value;
use crate::VarName;
use crate::dataflow::DataflowMonitor;
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::TypeCheckOptions;
use crate::lang::dsrv::ast::{DsrvSpecification, ExprRef};
use crate::lang::dsrv::expand::DsrvExpandError;
use crate::lang::dsrv::expand::language::{Dialect, LanguageError, LanguageRequest};
use crate::lang::dsrv::modules::{ModuleCollector, show_path};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::runtime_expression::{
    RuntimeExpressionError, RuntimeExpressionSite, is_runtime_expression,
};
use crate::lang::dsrv::source_map::{OwnedSite, SourceLabel};
use crate::lang::dsrv::span::Span;

use test_log::test;

const ROOT: &str = "use experimental::{modules, functions}\n";

/// Expand a program whose root is `root.dsrv` and whose modules are
/// supplied from `modules`, each labelled `<prefix><module>.dsrv`.
fn expand_labelled(
    root: &str,
    modules: &[(&str, &str)],
    prefix: &str,
) -> Result<DsrvSpecification, DsrvExpandError> {
    let label = |name: &str| SourceLabel::Path(format!("{prefix}{name}.dsrv").into());
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
    crate::lang::dsrv::expand::expand_program(
        collector.finish().expect("collected"),
        LanguageRequest::default(),
    )
}

fn expand(root: &str, modules: &[(&str, &str)]) -> Result<DsrvSpecification, DsrvExpandError> {
    expand_labelled(root, modules, "")
}

fn program(root: &str, modules: &[(&str, &str)]) -> DsrvSpecification {
    expand(root, modules).unwrap_or_else(|error| panic!("{root}: {error}"))
}

fn body(spec: &DsrvSpecification, var: &str) -> String {
    spec.var_expr_ref(&VarName::from(var))
        .expect("the stream is defined")
        .to_string()
}

fn evaluate_only_output(specification: DsrvSpecification) -> Value {
    let elaborated = specification
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(elaborated).unwrap();
    let mut output = vec![Value::NoVal; monitor.output_vars().len()];
    monitor.evaluate(&[], &mut output).unwrap();
    output.pop().unwrap()
}

fn runtime_node<'a>(spec: &'a DsrvSpecification, var: &str) -> ExprRef<'a> {
    spec.var_expr_ref(&VarName::from(var))
        .expect("the stream is defined")
        .postorder()
        .find(|node| is_runtime_expression(*node))
        .expect("a dynamic node")
}

/// What text supplied to `var`'s `dynamic` is read against.
fn site(spec: &DsrvSpecification, var: &str) -> RuntimeExpressionSite {
    RuntimeExpressionSite::unlocated(runtime_node(spec, var))
}

/// The experiment a refused program needed, and the text it was refused at.
fn needed<'a>(error: DsrvExpandError, root: &'a str) -> (&'static str, &'a str) {
    match error {
        DsrvExpandError::Language(LanguageError::NeedsExperiment { feature, span, .. }) => {
            (feature, &root[span.to_range()])
        }
        other => panic!("expected a missing experiment, got {other:?}"),
    }
}

fn text_at(root: &str, span: Span) -> &str {
    &root[span.to_range()]
}

// -----------------------------------------------------------------------------
// Whose header authorises syntax
// -----------------------------------------------------------------------------

const CASTING_LIB: &str = "use experimental::{modules, functions, casts}\n\
    def half(n: Int) -> Float = n as Float / 2.0\n\
    def good(n: Int) -> Int = n * 2\n";

const PLAIN_LIB: &str = "use experimental::{modules, functions}\n\
    def half(n: Int) -> Float = n as Float / 2.0\n\
    def good(n: Int) -> Int = n * 2\n";

#[test]
fn a_library_body_may_use_syntax_its_own_header_enables() {
    let spec = program(
        &format!("{ROOT}mod lib\nuse lib\nin x: Int\nout y: Float\ny = lib::half(x)\n"),
        &[("lib", CASTING_LIB)],
    );
    spec.check(TypeCheckOptions::STRICT)
        .discard_warnings()
        .expect("the library's cast checks");
}

#[test]
fn a_library_body_may_not_use_syntax_only_its_caller_enables() {
    let root = "use experimental::{modules, functions, casts}\nmod lib\nuse lib\n\
        in x: Int\nout y: Float\ny = lib::half(x)\n";
    let error = expand(root, &[("lib", PLAIN_LIB)]).expect_err("the library did not opt in");
    // Reported at the call, which is where inlined code is placed.
    assert_eq!(needed(error, root), ("casts", "lib::half(x)"));
}

#[test]
fn a_callers_argument_is_judged_by_the_callers_header() {
    let casting_root = "use experimental::{modules, functions, casts}\nmod lib\nuse lib\n\
        in x: Int\nout y: Int\ny = lib::good(x as Int)\n";
    program(casting_root, &[("lib", PLAIN_LIB)]);

    let plain_root =
        format!("{ROOT}mod lib\nuse lib\nin x: Int\nout y: Int\ny = lib::good(x as Int)\n");
    let error =
        expand(&plain_root, &[("lib", CASTING_LIB)]).expect_err("the caller did not opt in");
    assert_eq!(needed(error, &plain_root), ("casts", "x as Int"));
}

/// Code reached through a module in between is judged by the module that
/// wrote it, not by the one that relayed it.
#[test]
fn relayed_code_is_judged_by_the_module_that_wrote_it() {
    let mid = |experiments: &str| {
        format!(
            "use experimental::{{{experiments}}}\nmod leaf\nuse mid::leaf\n\
             def relay(n: Int) -> Float = mid::leaf::half(n)\n"
        )
    };
    let root = format!("{ROOT}mod mid\nuse mid\nin x: Int\nout y: Float\ny = mid::relay(x)\n");
    program(
        &root,
        &[
            ("mid", &mid("modules, functions")),
            ("mid::leaf", CASTING_LIB),
        ],
    );
    let error = expand(
        &root,
        &[
            ("mid", &mid("modules, functions, casts")),
            ("mid::leaf", PLAIN_LIB),
        ],
    )
    .expect_err("the leaf did not opt in");
    assert_eq!(needed(error, &root), ("casts", "mid::relay(x)"));
}

// -----------------------------------------------------------------------------
// Whose names a body resolves
// -----------------------------------------------------------------------------

#[test]
fn a_library_body_resolves_types_in_its_own_namespace() {
    let lib = "use experimental::{modules, functions}\ntype Meters = Int\n\
        def further(m: Meters) -> Meters = m + 1\n";
    let spec = program(
        &format!("{ROOT}mod lib\nuse lib\nin x: Int\nout y: Int\ny = lib::further(x)\n"),
        &[("lib", lib)],
    );
    spec.check(TypeCheckOptions::STRICT)
        .discard_warnings()
        .expect("Meters is the library's name");
}

/// A root alias of the same name does not change what the library meant.
#[test]
fn a_callers_alias_does_not_shadow_the_librarys() {
    let lib = "use experimental::{modules, functions, casts}\ntype Measure = Float\n\
        def scale(n: Int) -> Float = n as Measure\n";
    let spec = program(
        &format!(
            "{ROOT}mod lib\nuse lib\ntype Measure = Str\nin x: Int\nout y: Float\ny = lib::scale(x)\n"
        ),
        &[("lib", lib)],
    );
    assert!(
        body(&spec, "y").contains("as Float"),
        "{}",
        body(&spec, "y")
    );
    spec.check(TypeCheckOptions::STRICT)
        .discard_warnings()
        .expect("the library's Measure is a Float");
}

/// A bare tag is a constructor where its module took on tagged unions, and
/// its union is the one that module imported the tag from.
#[test]
fn a_library_constructor_resolves_in_the_librarys_namespace() {
    let lib = "use experimental::{modules, functions, tagged_unions}\n\
        type State = Union<Stopped, Moving: Int>\nuse self::State::*\n\
        def start(n: Int) -> State = Moving(n)\n";
    let spec = program(
        &format!("{ROOT}mod lib\nuse lib\nin x: Int\nout y: Any\ny = lib::start(x)\n"),
        &[("lib", lib)],
    );
    spec.check(TypeCheckOptions::STRICT)
        .discard_warnings()
        .expect("the library imported its own tags");
}

const CONSTANTS: &str = "use experimental::{modules, functions, constants}\n";

#[test]
fn a_library_body_names_its_own_constants_not_the_callers() {
    let lib = format!("{CONSTANTS}const factor: Int = 2\ndef scaled(n: Int) -> Int = n * factor\n");
    let spec = program(
        &format!(
            "{CONSTANTS}mod lib\nuse lib\nconst factor: Int = 100\n\
             in x: Int\nout y: Int\ny = lib::scaled(x)\n"
        ),
        &[("lib", &lib)],
    );
    let written = body(&spec, "y");
    assert!(
        written.contains('2') && !written.contains("100"),
        "{written}"
    );
}

#[test]
fn a_callers_constant_does_not_capture_a_library_parameter() {
    let lib = format!("{CONSTANTS}def inc(limit: Int) -> Int = limit + 1\n");
    let spec = program(
        &format!(
            "{CONSTANTS}mod lib\nuse lib\nconst limit: Int = 100\n\
             in x: Int\nout y: Int\ny = lib::inc(x)\n"
        ),
        &[("lib", &lib)],
    );
    assert!(!body(&spec, "y").contains("100"), "{}", body(&spec, "y"));
}

#[test]
fn definition_constants_do_not_capture_function_or_nested_binders() {
    let local = format!(
        "{}const n: Int = 100\n\
         def f() -> Int = n\n\
         def g(n: Int) -> Int = f()\n\
         out y: Int\ny = g(7)\n",
        ROOT.replace("functions}", "functions, constants}")
    );
    assert_eq!(evaluate_only_output(program(&local, &[])), Value::Int(100));
    let lambda = local.replace("y = g(7)", "y = (\\n: Int -> f())(7)");
    assert_eq!(evaluate_only_output(program(&lambda, &[])), Value::Int(100));

    let root = format!("{ROOT}mod lib\nuse lib\nout y: Int\ny = lib::read(2)\n");
    let function = format!(
        "{}const n: Int = 100\ndef read(n: Int) -> Int = n + 1\n",
        ROOT.replace("functions}", "functions, constants}")
    );
    assert_eq!(
        evaluate_only_output(program(&root, &[("lib", &function)])),
        Value::Int(3)
    );

    let lambda = function.replace("n + 1", "(\\n: Int -> n + 1)(n)");
    assert_eq!(
        evaluate_only_output(program(&root, &[("lib", &lambda)])),
        Value::Int(3)
    );

    let root = format!(
        "{}mod lib\nuse lib\n\
         type State = Union<Moving: Int>\nout y: Int\ny = lib::read(Moving(2))\n",
        ROOT.replace("functions}", "functions, tagged_unions}")
    );
    let matched = format!(
        "{}const n: Int = 100\n\
         type State = Union<Moving: Int>\n\
         def read(state: State) -> Int = match(state) {{ Moving(n) -> n + 1, }}\n",
        ROOT.replace(
            "functions}",
            "functions, constants, tagged_unions, pattern_matching}"
        )
    );
    assert_eq!(
        evaluate_only_output(program(&root, &[("lib", &matched)])),
        Value::Int(3)
    );

    let root = format!(
        "{}mod lib\nuse lib\n\
         type State = Union<Moving: Int>\nout y: Bool\ny = lib::is_two(Moving(2))\n",
        ROOT.replace("functions}", "functions, tagged_unions}")
    );
    let matched = format!(
        "{}const n: Int = 100\n\
         type State = Union<Moving: Int>\n\
         def is_two(state: State) -> Bool = matches(state, Moving(n) if n == 2)\n",
        ROOT.replace(
            "functions}",
            "functions, constants, tagged_unions, pattern_matching}"
        )
    );
    assert_eq!(
        evaluate_only_output(program(&root, &[("lib", &matched)])),
        Value::Bool(true)
    );
}

#[test]
fn a_callers_def_does_not_capture_a_library_parameter() {
    let lib = format!("{ROOT}def ap(f: (Int -> Int), n: Int) -> Int = f(n)\n");
    let spec = program(
        &format!(
            "{ROOT}mod lib\nuse lib\ndef f(n: Int) -> Int = n + 100\n\
             in x: Int\nout y: Int\ny = lib::ap(\\v -> v, x)\n"
        ),
        &[("lib", &lib)],
    );
    assert!(!body(&spec, "y").contains("100"), "{}", body(&spec, "y"));
}

/// An offset naming a constant is folded where it was written, too.
#[test]
fn a_library_offset_names_its_own_constant() {
    let lib = format!("{CONSTANTS}const back: Int = 1\ndef prev(n: Int) -> Int = n[back]\n");
    let spec = program(
        &format!(
            "{CONSTANTS}mod lib\nuse lib\nconst back: Int = 7\n\
             in x: Int\nout y: Int\ny = lib::prev(x)\n"
        ),
        &[("lib", &lib)],
    );
    let written = body(&spec, "y");
    assert!(
        written.contains("[1]") && !written.contains("[7]"),
        "{written}"
    );
}

// -----------------------------------------------------------------------------
// Text supplied at run time
// -----------------------------------------------------------------------------

const HELPER_LIB: &str = "use experimental::{modules, functions, casts}\n\
    def helper(n: Int) -> Int = n * 10\n\
    def read(s: Str) -> Int = dynamic(s: Int)\n";

fn helper_program() -> DsrvSpecification {
    program(
        &format!(
            "{ROOT}mod lib\nuse lib\ndef helper(n: Int) -> Int = n + 1\n\
             in s: Str\nout y: Int\nout z: Int\ny = lib::read(s)\nz = dynamic(s: Int)\n"
        ),
        &[("lib", HELPER_LIB)],
    )
}

#[test]
fn text_supplied_to_a_library_dynamic_calls_the_librarys_helper() {
    let spec = helper_program();
    let library = site(&spec, "y").parse("helper(2)").expect("parses");
    let caller = site(&spec, "z").parse("helper(2)").expect("parses");
    assert!(library.to_string().contains("* 10"), "{library}");
    assert!(caller.to_string().contains("+ 1"), "{caller}");
}

#[test]
fn text_supplied_to_a_library_dynamic_is_judged_by_the_librarys_header() {
    let spec = helper_program();
    site(&spec, "y")
        .parse("2.5 as Int")
        .expect("the library opted into casts");
    assert!(matches!(
        site(&spec, "z").parse("2.5 as Int"),
        Err(RuntimeExpressionError::Parse { .. })
    ));
}

#[test]
fn a_library_dynamic_carries_the_librarys_environment() {
    let spec = helper_program();
    let library = site(&spec, "y");
    let caller = site(&spec, "z");
    assert!(!library.same_lexical_environment(&caller));
    assert_eq!(
        library
            .context()
            .language()
            .experiment_names()
            .collect::<Vec<_>>(),
        ["modules", "functions", "casts"]
    );
    assert_eq!(
        caller
            .context()
            .language()
            .experiment_names()
            .collect::<Vec<_>>(),
        ["modules", "functions"]
    );
}

#[test]
fn text_supplied_to_a_library_dynamic_checks_and_runs_the_librarys_helper() {
    let elaborated = helper_program()
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .expect("checks");
    let prepared = |var: &str| {
        elaborated
            .var_expr_ref(&VarName::from(var))
            .unwrap()
            .postorder()
            .find(|node| is_runtime_expression(node.expr()))
            .expect("a dynamic node")
            .runtime_expression()
            .clone()
    };
    let library = prepared("y").parse_and_check("helper(2)").expect("checks");
    let caller = prepared("z").parse_and_check("helper(2)").expect("checks");
    assert!(library.as_ref().expr().to_string().contains("* 10"));
    assert!(caller.as_ref().expr().to_string().contains("+ 1"));
}

// -----------------------------------------------------------------------------
// Admission
// -----------------------------------------------------------------------------

/// A library that names no dialect is read inside the program's: its
/// runtime text is admitted exactly as the program's own is.
#[test]
fn a_library_dynamic_keeps_the_programs_dialect() {
    let spec = program(
        &format!(
            "language distributed\n{ROOT}mod lib\nuse lib\n\
             in s: Str\nout y: Int\ny = lib::read(s)\n"
        ),
        &[("lib", HELPER_LIB)],
    );
    let library = site(&spec, "y");
    assert_eq!(library.context().language().dialect(), Dialect::Distributed);
    library
        .parse("monitored_at(s, A)")
        .expect("the program is distributed");
}

#[test]
fn a_library_body_is_admitted_by_the_programs_dialect() {
    let lib = "use experimental::{modules, functions}\n\
        def here(v: Str) -> Bool = monitored_at(v, A)\n";
    let distributed = format!(
        "language distributed\n{ROOT}mod lib\nuse lib\nin s: Str\nout y: Bool\ny = lib::here(s)\n"
    );
    program(&distributed, &[("lib", lib)]);
    let full = format!("{ROOT}mod lib\nuse lib\nin s: Str\nout y: Bool\ny = lib::here(s)\n");
    assert!(matches!(
        expand(&full, &[("lib", lib)]),
        Err(DsrvExpandError::Language(LanguageError::NeedsDistributed { span, .. }))
            if text_at(&full, span) == "lib::here(s)"
    ));
}

// -----------------------------------------------------------------------------
// Diagnostics
// -----------------------------------------------------------------------------

fn named(site: Option<&OwnedSite>) -> Option<(String, String)> {
    site.map(|site| {
        (
            site.label().to_string(),
            site.snippet().expect("a span within its file").to_owned(),
        )
    })
}

/// Code read in its own environment is still reported at the call, with a
/// note naming where it was written.
#[test]
fn a_finding_in_library_code_is_primary_at_the_call_with_a_definition_note() {
    let lib = "use experimental::{modules, functions, casts}\ntype Meters = Int\n\
        def bad(m: Meters) -> Meters = (m as Float) + true\n";
    let errors = program(
        &format!("{ROOT}mod lib\nuse lib\nin x: Int\nout y: Int\ny = lib::bad(x)\n"),
        &[("lib", lib)],
    )
    .check(TypeCheckOptions::STRICT)
    .discard_warnings()
    .expect_err("bad does not check");
    let location = errors[0].location();
    assert_eq!(
        named(location.primary()),
        Some(("root.dsrv".to_owned(), "lib::bad(x)".to_owned()))
    );
    let (label, text) = named(location.definition()).expect("a definition note");
    assert_eq!(label, "lib.dsrv");
    assert!("(m as Float) + true".contains(&text), "defined at {text:?}");
}

// -----------------------------------------------------------------------------
// Identity
// -----------------------------------------------------------------------------

/// What the dataflow descriptor records of `var`'s runtime-expression site.
fn identity(spec: &DsrvSpecification, var: &str) -> String {
    let site = site(spec, var);
    let mut described = serde_json::to_string(site.context().fingerprint()).unwrap();
    site.callable().describe(&mut described);
    described
}

fn helper_root() -> String {
    format!(
        "{ROOT}mod lib\nuse lib\ndef helper(n: Int) -> Int = n + 1\n\
         in s: Str\nout y: Int\nout z: Int\ny = lib::read(s)\nz = dynamic(s: Int)\n"
    )
}

#[test]
fn a_library_sites_identity_follows_the_librarys_settings_and_not_its_label() {
    let root = helper_root();
    // Headers of one length, so that changing the settings moves no span.
    let lib = HELPER_LIB.replace("casts}", "casts,  generics}");
    let original = program(&root, &[("lib", &lib)]);
    let relabelled = expand_labelled(&root, &[("lib", &lib)], "elsewhere/").expect("expands");
    assert_eq!(identity(&original, "y"), identity(&relabelled, "y"));
    assert_eq!(identity(&original, "z"), identity(&relabelled, "z"));
    assert!(!identity(&original, "y").contains("lib.dsrv"));

    // An experiment nothing uses is still a different reading of the text.
    let resettled = lib.replace("casts,  generics}", "casts, constants}");
    let changed = program(&root, &[("lib", &resettled)]);
    assert_ne!(identity(&original, "y"), identity(&changed, "y"));
    // The caller's site can call the library's defs, so it changes too.
    assert_ne!(identity(&original, "z"), identity(&changed, "z"));
}

/// The library's site means what the library wrote; the caller's settings
/// are not part of it.
#[test]
fn a_library_sites_identity_does_not_follow_the_callers_settings() {
    let root = helper_root();
    let original = program(&root, &[("lib", HELPER_LIB)]);
    let resettled = program(
        &root.replacen("functions}", "functions, generics}", 1),
        &[("lib", HELPER_LIB)],
    );
    assert_eq!(identity(&original, "y"), identity(&resettled, "y"));
    assert_ne!(identity(&original, "z"), identity(&resettled, "z"));
}

#[test]
fn a_callers_site_identity_includes_helpers_of_nested_runtime_sites() {
    let root = format!("{ROOT}mod lib\nuse lib\nin s: Str\nout y: Int\ny = dynamic(s: Int)\n");
    let library = |operator| {
        format!(
            "{ROOT}internal def helper(n: Int) -> Int = n {operator} 2\n\
             def read(s: Str) -> Int = dynamic(s: Int)\n"
        )
    };
    let doubled = program(&root, &[("lib", &library("*"))]);
    let increased = program(&root, &[("lib", &library("+"))]);

    assert_ne!(identity(&doubled, "y"), identity(&increased, "y"));
}

#[test]
fn nested_runtime_sites_capture_their_transitive_callable_sources() {
    let root = format!("{ROOT}mod lib\nuse lib\nin s: Str\nout y: Int\ny = dynamic(s: Int)\n");
    let lib = format!(
        "{ROOT}mod leaf\nmod spare\nuse lib::leaf::*\nuse lib::spare\n\
         def read(s: Str) -> Int = dynamic(s: Int)\n"
    );
    let leaf = format!("{ROOT}def helper(n: Int) -> Int = n * 2\n");
    let spare = format!("{ROOT}def alternative(n: Int) -> Int = n + 1\n");
    let specification = program(
        &root,
        &[("lib", &lib), ("lib::leaf", &leaf), ("lib::spare", &spare)],
    );
    let elaborated = specification
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let outer = elaborated
        .var_expr_ref(&VarName::new("y"))
        .unwrap()
        .runtime_expression()
        .clone();

    let accepted = outer
        .parse_and_check("lib::read(\"helper(1)\")")
        .expect("the accepted expression prepares its nested library site");
    assert!(
        accepted
            .as_ref()
            .postorder()
            .filter(|node| is_runtime_expression(node.expr()))
            .all(|node| node.runtime_expression().sources().is_some())
    );
}

// -----------------------------------------------------------------------------
// Threads
// -----------------------------------------------------------------------------

#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(
    crate::lang::dsrv::expand::functions::Callable: Send,
    Sync
);

#[cfg(feature = "thread-safe-ast")]
#[test]
fn a_library_site_accepts_text_on_another_thread_after_the_program_is_dropped() {
    let elaborated = helper_program()
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .expect("checks");
    let site = elaborated
        .var_expr_ref(&VarName::from("y"))
        .unwrap()
        .postorder()
        .find(|node| is_runtime_expression(node.expr()))
        .expect("a dynamic node")
        .runtime_expression()
        .clone();
    drop(elaborated);
    let body = std::thread::spawn(move || {
        site.parse_and_check("helper(2) + trunc(2.5)")
            .expect("the library's helper and casts")
            .as_ref()
            .expr()
            .to_string()
    })
    .join()
    .unwrap();
    assert!(body.contains("* 10"), "{body}");
}

// -----------------------------------------------------------------------------
// `defer`
// -----------------------------------------------------------------------------

/// A `defer` a library wrote is a runtime-expression site like a `dynamic`,
/// and carries the same environment: the library's helper and the library's
/// syntax, not the caller's.
#[test]
fn text_supplied_to_a_library_defer_is_read_where_the_library_wrote_it() {
    let lib = "use experimental::{modules, functions, casts}\n\
        def helper(n: Int) -> Int = n * 10\n\
        def wait(s: Str) -> Int = defer(s: Int)\n";
    let spec = program(
        &format!(
            "{ROOT}mod lib\nuse lib\ndef helper(n: Int) -> Int = n + 1\n\
             in s: Str\nout y: Int\nout z: Int\ny = lib::wait(s)\nz = defer(s: Int)\n"
        ),
        &[("lib", lib)],
    );
    let library = site(&spec, "y");
    let caller = site(&spec, "z");
    assert!(!library.same_lexical_environment(&caller));

    let supplied = library.parse("helper(2)").expect("parses");
    assert!(supplied.to_string().contains("* 10"), "{supplied}");
    assert!(
        caller
            .parse("helper(2)")
            .expect("parses")
            .to_string()
            .contains("+ 1")
    );

    library
        .parse("2.5 as Int")
        .expect("the library opted into casts");
    assert!(matches!(
        caller.parse("2.5 as Int"),
        Err(RuntimeExpressionError::Parse { .. })
    ));
}
