//! Nominal newtypes (AC-NT): declaration, identity, ownership, the `Any`
//! boundary, elaboration and evaluation on both runtimes.
//!
//! Most cases use a toy timing library of two modules: `app`, which
//! declares the nullary domain newtypes `JournalTime` and `Producer`, and
//! `time`, which declares the generic newtypes `Instant<D>` and
//! `Duration<D>` over `Float` with the checked factories and accessors an
//! importer calls. Neither is the planned typed timing library.

use std::collections::BTreeMap;
use std::rc::Rc;

use contiguous_tree::TreeCursorExt;
use futures::FutureExt;
use smol::LocalExecutor;

use crate::VarName;
use crate::core::{
    ExecutionPolicy, JsonStreamValue, NewtypeValue, OutputInterface, RuntimeSpec, Semantics,
    StreamType, Value,
};
use crate::dataflow::{DataflowMonitor, TypedDataflowMonitor};
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::ElaboratedDsrvSpecification;
use crate::lang::dsrv::TypeCheckOptions;
use crate::lang::dsrv::ast::{DsrvSpecification, ExprKind};
use crate::lang::dsrv::diagnostics::{SemanticError, TypeErrorKind, UnresolvedTypeKind};
use crate::lang::dsrv::expand::DsrvExpandError;
use crate::lang::dsrv::expand::language::LanguageError;
use crate::lang::dsrv::modules::{ModuleCollector, show_path};
use crate::lang::dsrv::parser::{DsrvParseError, parse_str};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::runtime_expression::{
    RuntimeExpressionError, RuntimeExpressionSite, is_runtime_expression,
};
use crate::lang::dsrv::source::SourceResolveError;
use crate::runtime::builder::GeneralRuntimeBuilder;

use test_log::test;

const BOTH: [TypeCheckOptions; 2] = [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL];

// -----------------------------------------------------------------------------
// The toy library and helpers
// -----------------------------------------------------------------------------

const APP: &str = "\
use experimental::{newtypes}
newtype JournalTime = Unit
newtype Producer = Unit
";

const TIME: &str = "\
use experimental::{modules, functions, generics, newtypes}
newtype Instant<D> = Float
newtype Duration<D> = Float
// A domain of this module's own, for its runtime expression site.
newtype Local = Unit

def at<D>(x: Float) -> Instant<D> = Instant::new(x)
// The checked factory: a negative amount is clamped to zero.
def seconds<D>(x: Float) -> Duration<D> =
  if x >= 0.0 then Duration::new(x) else Duration::new(0.0)
def secs<D>(d: Duration<D>) -> Float = Duration::value(d)
def since<D>(start: Instant<D>, end: Instant<D>) -> Duration<D> =
  Duration::new(Instant::value(end) - Instant::value(start))
def later<D>(a: Instant<D>, b: Instant<D>) -> Bool = Instant::value(b) > Instant::value(a)
// A factory in that domain, which needs nothing inferred.
def local_seconds(x: Float) -> Duration<Local> = Duration::new(x)
// An unchecked factory the module keeps to itself.
internal def raw_seconds(x: Float) -> Duration<Local> = Duration::new(x)
// A library-authored runtime expression site.
def read(s: Str) -> Duration<Local> = dynamic(s: Duration<Local>)
";

const ROOT_HEADER: &str = "\
use experimental::{modules, functions, generics, newtypes, casts}
mod app
mod time
use app
use time
";

const ROOT_TYPES: &str = "\
type J = app::JournalTime
type DJ = time::Duration<J>
type IJ = time::Instant<app::JournalTime>
type IP = time::Instant<app::Producer>
";

const JOURNAL_DURATION: &str = "time::Duration<app::JournalTime>";

fn library() -> [(&'static str, &'static str); 2] {
    [("app", APP), ("time", TIME)]
}

/// Expand a program whose root is `root` and whose modules are supplied
/// from `modules`.
fn expand(root: &str, modules: &[(&str, &str)]) -> Result<DsrvSpecification, DsrvExpandError> {
    let mut collector = ModuleCollector::new(root).expect("a parsable root");
    while let Some(module) = collector.next_request().map(<[ModuleName]>::to_vec) {
        let wanted = show_path(&module);
        let text = modules
            .iter()
            .find(|(name, _)| *name == wanted)
            .unwrap_or_else(|| panic!("no source for {wanted}"))
            .1;
        collector.supply(text).expect("a parsable module");
    }
    crate::lang::dsrv::expand::expand_program(collector.finish().expect("collected"), None)
}

fn program(body: &str) -> DsrvSpecification {
    let root = format!("{ROOT_HEADER}{ROOT_TYPES}{body}");
    expand(&root, &library()).unwrap_or_else(|error| panic!("{root}: {error}"))
}

fn program_error(body: &str) -> DsrvExpandError {
    let root = format!("{ROOT_HEADER}{ROOT_TYPES}{body}");
    match expand(&root, &library()) {
        Ok(_) => panic!("{root}: expected expansion to fail"),
        Err(error) => error,
    }
}

fn check_errors(spec: DsrvSpecification, options: TypeCheckOptions) -> Vec<SemanticError> {
    spec.check(options)
        .without_warnings()
        .err()
        .unwrap_or_else(|| panic!("expected checking to fail ({:?})", options.mode))
}

fn elaborate(spec: DsrvSpecification, options: TypeCheckOptions) -> ElaboratedDsrvSpecification {
    spec.check_and_elaborate(options)
        .without_warnings()
        .unwrap_or_else(|errors| panic!("{errors:?}"))
}

fn annotation(spec: &DsrvSpecification, var: &str) -> StreamType {
    spec.type_annotation(&VarName::from(var))
        .unwrap_or_else(|| panic!("{var} has no annotation"))
        .clone()
}

fn brand(spec: &DsrvSpecification, var: &str) -> String {
    match annotation(spec, var) {
        StreamType::Newtype(schema) => schema.brand().to_string(),
        other => panic!("{var} is not a newtype: {other}"),
    }
}

fn is_type_error(errors: &[SemanticError]) -> bool {
    errors
        .iter()
        .any(|error| matches!(error, SemanticError::TypeError(_)))
}

fn value_of(brand: &str, value: f64) -> Value {
    NewtypeValue::new(brand, Value::Float(value)).into()
}

/// Run the elaborated `spec` on `runtime` over `inputs`, one value per tick
/// for each input, returning each tick's outputs and the failure that
/// stopped it, if any. A failure is an error from the runtime or a panic in
/// its evaluation, which is how both runtimes report a failed operation.
fn run(
    runtime: RuntimeSpec,
    semantics: Semantics,
    spec: ElaboratedDsrvSpecification,
    inputs: BTreeMap<VarName, Vec<Value>>,
) -> (Vec<BTreeMap<VarName, Value>>, Option<String>) {
    let executor = Rc::new(LocalExecutor::new());
    let (sender, mut receiver) = crate::io::channel::output(1024);
    let outputs = spec.output_vars().clone();
    let run = {
        let executor = Rc::clone(&executor);
        async move {
            let writer = crate::io::channel::open_output(
                sender,
                OutputInterface::outputs(outputs.iter().cloned()).expect("an interface"),
            )
            .await
            .expect("the channel opens");
            let monitor = GeneralRuntimeBuilder::new()
                .executor(Rc::clone(&executor))
                .model(spec)
                .input(crate::io::map::input_stream(inputs))
                .output_writer(writer)
                .runtime(runtime)
                .semantics(semantics)
                .build()
                .await
                .expect("the runtime builds");
            crate::core::Runtime::run(monitor).await
        }
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        smol::block_on(executor.run(std::panic::AssertUnwindSafe(run).catch_unwind()))
    }));
    let failure = match outcome {
        Ok(Ok(Ok(()))) => None,
        Ok(Ok(Err(error))) => Some(format!("{error:#}")),
        Ok(Err(panic)) | Err(panic) => Some(panic_message(panic)),
    };
    let mut rows = Vec::new();
    while let Ok(row) = receiver.try_recv() {
        rows.push(row);
    }
    (rows, failure)
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
        .unwrap_or_else(|| "a panic with no message".to_owned())
}

/// The checked semantics a specification checked with `options` runs
/// under by default.
fn semantics_for(spec: &ElaboratedDsrvSpecification) -> Semantics {
    match spec.check_mode() {
        crate::lang::dsrv::TypeCheckMode::Strict => Semantics::TypedUntimed,
        crate::lang::dsrv::TypeCheckMode::Gradual => Semantics::GradualTypedUntimed,
    }
}

/// Run `spec` on the dataflow and semi-sync runtimes under the checked
/// semantics its check mode selects. They must agree on every output of
/// every tick and on whether they fail.
///
/// On failure, the synchronous dataflow runtime has published every tick
/// before the failing one, which is what the returned rows are; semi-sync
/// publishes asynchronously and may not have written its last completed
/// tick when the failure stops it, so its rows need only be a prefix of
/// them. Which tick failed is established from the failure itself: callers
/// check that it names the value that arrived at that tick.
fn run_both(
    spec: ElaboratedDsrvSpecification,
    inputs: BTreeMap<VarName, Vec<Value>>,
) -> (Vec<BTreeMap<VarName, Value>>, Option<String>) {
    let semantics = semantics_for(&spec);
    run_both_under(semantics, spec, inputs)
}

fn run_both_under(
    semantics: Semantics,
    spec: ElaboratedDsrvSpecification,
    inputs: BTreeMap<VarName, Vec<Value>>,
) -> (Vec<BTreeMap<VarName, Value>>, Option<String>) {
    let (dataflow_rows, dataflow_failure) = run(
        RuntimeSpec::Dataflow(ExecutionPolicy::Synchronous),
        semantics,
        spec.clone(),
        inputs.clone(),
    );
    let (semi_rows, semi_failure) = run(RuntimeSpec::SemiSync, semantics, spec, inputs);
    assert_eq!(
        dataflow_failure.is_some(),
        semi_failure.is_some(),
        "only one runtime failed: dataflow {dataflow_failure:?}, semi-sync {semi_failure:?}"
    );
    match (&dataflow_failure, &semi_failure) {
        (Some(dataflow), Some(semi)) => {
            assert!(
                dataflow_rows.starts_with(&semi_rows),
                "semi-sync published {semi_rows:?}, which dataflow did not: {dataflow_rows:?}"
            );
            // Both report the same failed operation on the same value.
            assert_eq!(
                failed_operation(dataflow),
                failed_operation(semi),
                "the runtimes fail differently"
            );
        }
        _ => assert_eq!(dataflow_rows, semi_rows, "the runtimes disagree on outputs"),
    }
    (dataflow_rows, dataflow_failure)
}

/// The operation error inside a runtime's failure report, which each
/// runtime wraps in its own context.
fn failed_operation(failure: &str) -> &str {
    ["cannot cast", "cannot take the value of"]
        .iter()
        .find_map(|start| failure.find(start).map(|at| &failure[at..]))
        .map(|error| error.lines().next().unwrap_or(error))
        .unwrap_or(failure)
}

fn inputs<const N: usize>(values: [(&str, Vec<Value>); N]) -> BTreeMap<VarName, Vec<Value>> {
    values
        .into_iter()
        .map(|(name, values)| (VarName::from(name), values))
        .collect()
}

fn floats(values: &[f64]) -> Vec<Value> {
    values.iter().copied().map(Value::Float).collect()
}

// -----------------------------------------------------------------------------
// Declaration and the experiment
// -----------------------------------------------------------------------------

#[test]
fn a_newtype_needs_the_newtypes_experiment() {
    match parse_str("newtype Meters = Float\nin x: Meters\n") {
        Err(DsrvParseError::Language(LanguageError::NeedsExperiment { feature, .. })) => {
            assert_eq!(feature, "newtypes");
        }
        other => panic!("expected the experiment to be needed, got {other:?}"),
    }
    parse_str("use experimental::{newtypes}\nnewtype Meters = Float\nin x: Meters\n")
        .expect("the experiment admits the declaration");
    // `*` and `high_level_dsrv` both include it.
    parse_str("use experimental::*\nnewtype Meters = Float\nin x: Meters\n").unwrap();
    parse_str("use experimental::high_level_dsrv\nnewtype Meters = Float\nin x: Meters\n").unwrap();
}

#[test]
fn a_newtype_resolves_to_its_brand_and_representation() {
    let spec = parse_str(
        "use experimental::{newtypes}\nnewtype Meters = Float\nin x: Meters\nout y: Meters = x\n",
    )
    .unwrap();
    let StreamType::Newtype(schema) = annotation(&spec, "x") else {
        panic!("expected a newtype");
    };
    // A declaration of the root module has no module prefix.
    assert_eq!(schema.brand(), "Meters");
    assert_eq!(schema.repr(), &StreamType::Float);
}

// AC-NT-8: `type` stays structural.
#[test]
fn a_type_alias_is_still_structural() {
    let spec = parse_str(
        "use experimental::{newtypes}\ntype Meters = Float\nin x: Meters\nout y: Float = x\n",
    )
    .unwrap();
    assert_eq!(annotation(&spec, "x"), StreamType::Float);
    for options in BOTH {
        elaborate(spec.clone(), options);
    }
}

// -----------------------------------------------------------------------------
// AC-NT-1: distinct declarations
// -----------------------------------------------------------------------------

#[test]
fn two_newtypes_over_one_representation_are_not_interchangeable() {
    let header = "use experimental::{newtypes}\nnewtype A = Float\nnewtype B = Float\n";
    for body in [
        "in a: A\nout b: B = a\n",
        // Nor is a newtype its representation, in either direction.
        "in a: A\nout f: Float = a\n",
        "in f: Float\nout a: A = f\n",
        // Operations typed for the representation do not apply.
        "in a: A\nout f: A = a + a\n",
        // Comparing across the two is a check error, not `false`.
        "in a: A\nin b: B\nout same: Bool = a == b\n",
    ] {
        let source = format!("{header}{body}");
        for options in BOTH {
            let errors = check_errors(parse_str(&source).unwrap(), options);
            assert!(is_type_error(&errors), "{source}: {errors:?}");
        }
    }
    // The same newtype compares with itself.
    let source = format!("{header}in a: A\nin c: A\nout same: Bool = a == c\n");
    for options in BOTH {
        elaborate(parse_str(&source).unwrap(), options);
    }
}

#[test]
fn new_and_value_of_the_wrong_type_are_check_errors() {
    let header = "use experimental::{newtypes}\nnewtype A = Float\nnewtype B = Float\n";
    for body in [
        "in s: Str\nout a: A = A::new(s)\n",
        "in b: B\nout f: Float = A::value(b)\n",
        "in f: Float\nout g: Float = A::value(f)\n",
        "in f: Float\nout b: B = A::new(f)\n",
    ] {
        let source = format!("{header}{body}");
        for options in BOTH {
            let errors = check_errors(parse_str(&source).unwrap(), options);
            assert!(
                errors.iter().any(|error| matches!(
                    error,
                    SemanticError::TypeError(error)
                        if matches!(error.kind(),
                            TypeErrorKind::NewtypeMismatch | TypeErrorKind::AnnotationTypeMismatch)
                )),
                "{source}: {errors:?}"
            );
        }
    }
}

// -----------------------------------------------------------------------------
// AC-NT-2: phantom generic arguments
// -----------------------------------------------------------------------------

#[test]
fn instances_in_different_domains_are_distinct_brands() {
    let spec = program("in j: IJ\nin p: IP\nout same_j: IJ = j\n");
    assert_eq!(brand(&spec, "j"), "time::Instant<app::JournalTime>");
    assert_eq!(brand(&spec, "p"), "time::Instant<app::Producer>");
    for options in BOTH {
        let errors = check_errors(program("in j: IJ\nout p: IP = j\n"), options);
        assert!(is_type_error(&errors), "{errors:?}");
    }
}

#[test]
fn a_generic_def_infers_a_phantom_argument_from_its_arguments() {
    let spec = program(
        "in x: Float\n\
         aux start: IJ = time::at(0.0)\n\
         aux now: IJ = time::at(x)\n\
         out elapsed: DJ = time::since(start, now)\n\
         out moved: Bool = time::later(start, now)\n",
    );
    for options in BOTH {
        let elaborated = elaborate(spec.clone(), options);
        let elapsed = elaborated
            .var_expr_ref(&VarName::from("elapsed"))
            .unwrap()
            .typ()
            .to_string();
        assert_eq!(elapsed, JOURNAL_DURATION);
        let (rows, failure) = run_both(elaborated, inputs([("x", floats(&[2.5]))]));
        assert_eq!(failure, None);
        assert_eq!(
            rows[0][&VarName::from("elapsed")],
            value_of(JOURNAL_DURATION, 2.5)
        );
        assert_eq!(rows[0][&VarName::from("moved")], Value::Bool(true));
    }
}

#[test]
fn a_phantom_argument_is_inferred_through_a_generic_alias_nested_in_itself() {
    // `D` occurs only inside a second use of `Box`, under the first. The
    // parameter is still inferred from the argument alone: the result is a
    // `Bool` and says nothing about it.
    let spec = program(
        "type Box<T> = Struct<item: T>\n\
         def moved<D>(b: Box<Box<time::Instant<D>>>, now: time::Instant<D>) -> Bool =\n\
           time::later(b.item.item, now)\n\
         in x: Float\n\
         aux start: IJ = time::at(1.0)\n\
         aux boxed: Box<Box<IJ>> = { item: { item: start } }\n\
         aux now: IJ = time::at(x)\n\
         out moved: Bool = moved(boxed, now)\n",
    );
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(spec.clone(), options),
            inputs([("x", floats(&[0.5, 2.0]))]),
        );
        assert_eq!(failure, None);
        let moved = rows
            .iter()
            .map(|row| row[&VarName::from("moved")].clone())
            .collect::<Vec<_>>();
        assert_eq!(moved, [Value::Bool(false), Value::Bool(true)]);
    }
}

#[test]
fn a_stream_function_infers_a_phantom_argument_from_a_newtype_stream() {
    const TRACK: &str = r#"use experimental::{modules, functions, generics, local_streams, newtypes}
use time
// The latest instant seen so far.
def latest<D>(now: time::Instant<D>) -> time::Instant<D> = result
where {
  aux previous: time::Instant<D> = default(result[1], now)
  aux result: time::Instant<D> = if time::later(previous, now) then now else previous
}
"#;
    let root = r#"use experimental::{modules, functions, generics, local_streams, newtypes}
mod app
mod time
mod track
use app
use time
use track
type IJ = time::Instant<app::JournalTime>
in x: Float
aux now: IJ = time::at(x)
out latest = track::latest(now)
"#;
    let spec = expand(root, &[("app", APP), ("time", TIME), ("track", TRACK)])
        .unwrap_or_else(|error| panic!("{error}"));
    for options in BOTH {
        let elaborated = elaborate(spec.clone(), options);
        let latest = elaborated
            .var_expr_ref(&VarName::from("latest"))
            .unwrap()
            .typ()
            .to_string();
        assert_eq!(latest, "time::Instant<app::JournalTime>");
        let (rows, failure) = run_both(elaborated, inputs([("x", floats(&[1.0, 3.0, 2.0]))]));
        assert_eq!(failure, None);
        let latest = rows
            .iter()
            .map(|row| row[&VarName::from("latest")].clone())
            .collect::<Vec<_>>();
        let instant = |at| value_of("time::Instant<app::JournalTime>", at);
        assert_eq!(latest, [instant(1.0), instant(3.0), instant(3.0)]);
    }
}

#[test]
fn a_call_mixing_domains_fails_at_check() {
    let body = "in j: IJ\nin p: IP\nout moved: Bool = time::later(j, p)\n";
    for options in BOTH {
        let errors = check_errors(program(body), options);
        assert!(is_type_error(&errors), "{errors:?}");
    }
}

#[test]
fn a_phantom_argument_with_nothing_to_infer_it_from_asks_for_an_annotation() {
    // Nothing says which domain the instants are in.
    let body = "in x: Float\nout moved: Bool = time::later(time::at(x), time::at(x))\n";
    for options in BOTH {
        let errors = check_errors(program(body), options);
        assert!(
            errors
                .iter()
                .any(|error| format!("{error:?}").contains("annotate")),
            "{errors:?}"
        );
    }
    // The same inside the declaring module, at the operation itself.
    let source = "use experimental::{newtypes, generics}\n\
                  newtype Instant<D> = Float\n\
                  in x: Float\n\
                  out v: Float = Instant::value(Instant::new(x))\n";
    for options in BOTH {
        let errors = check_errors(parse_str(source).unwrap(), options);
        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::UnresolvedType(error)
                    if *error.kind() == UnresolvedTypeKind::NewtypeArguments
            )),
            "{errors:?}"
        );
    }
}

// -----------------------------------------------------------------------------
// AC-NT-3: imports
// -----------------------------------------------------------------------------

#[test]
fn identity_survives_aliases_and_another_import_path() {
    // `J` is a structural alias of the domain; `Stamp` reaches the same
    // instance through a glob import and a different spelling.
    let root = format!(
        "{ROOT_HEADER}use time::*\n{ROOT_TYPES}\
         type Stamp = Instant<app::JournalTime>\n\
         in a: IJ\nin b: time::Instant<J>\nin c: Stamp\n\
         out same: Bool = a == b and b == c\n"
    );
    let spec = expand(&root, &library()).unwrap();
    assert_eq!(brand(&spec, "a"), brand(&spec, "b"));
    assert_eq!(brand(&spec, "b"), brand(&spec, "c"));
    for options in BOTH {
        elaborate(spec.clone(), options);
    }
    // A module that names the instance under its own alias.
    let clock = "use experimental::{modules, generics}\nuse app\nuse time\n\
                 type Tick = time::Instant<app::JournalTime>\n";
    let root = format!(
        "{}mod clock\nuse app\nuse time\nuse clock\n{ROOT_TYPES}\
         in a: IJ\nin t: clock::Tick\nout same: Bool = a == t\n",
        ROOT_HEADER.replace("use app\nuse time\n", "")
    );
    let modules = [("app", APP), ("time", TIME), ("clock", clock)];
    let spec = expand(&root, &modules).unwrap();
    assert_eq!(brand(&spec, "t"), "time::Instant<app::JournalTime>");
    for options in BOTH {
        elaborate(spec.clone(), options);
    }
}

#[test]
fn same_named_declarations_in_two_modules_are_distinct() {
    let a = "use experimental::{newtypes}\nnewtype Meters = Float\n";
    let b = "use experimental::{newtypes}\nnewtype Meters = Float\n";
    let root = "use experimental::{modules}\nmod a\nmod b\nuse a\nuse b\n\
                in x: a::Meters\nout y: b::Meters = x\n";
    let spec = expand(root, &[("a", a), ("b", b)]).unwrap();
    assert_eq!(brand(&spec, "x"), "a::Meters");
    assert_eq!(brand(&spec, "y"), "b::Meters");
    for options in BOTH {
        assert!(is_type_error(&check_errors(spec.clone(), options)));
    }
}

// -----------------------------------------------------------------------------
// AC-NT-5: ownership
// -----------------------------------------------------------------------------

#[test]
fn only_the_declaring_module_builds_or_reads_a_representation() {
    for body in [
        "in x: Float\nout d: DJ = time::Duration::new(x)\n",
        "in d: DJ\nout x: Float = time::Duration::value(d)\n",
        // An alias of the instance grants nothing.
        "in x: Float\nout d: DJ = DJ::new(x)\n",
        "in d: DJ\nout x: Float = DJ::value(d)\n",
        // Nor does naming another module's nullary newtype.
        "out j: app::JournalTime = app::JournalTime::new(())\n",
    ] {
        assert!(
            matches!(
                program_error(body),
                DsrvExpandError::Resolve(SourceResolveError::NewtypeRepresentation { .. })
            ),
            "{body}"
        );
    }
}

#[test]
fn an_exported_factory_works_after_import_and_inlining() {
    let spec = program(
        "in x: Float\n\
         aux d: DJ = time::seconds(x)\n\
         out s: Float = time::secs(d)\n\
         out d_out: DJ = d\n",
    );
    // The factory's body was inlined into the root, `new` and all.
    assert!(
        spec.var_expr_ref(&VarName::from("d"))
            .unwrap()
            .postorder()
            .any(|node| matches!(node.kind(), ExprKind::Newtype(..))),
        "the factory is inlined"
    );
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(spec.clone(), options),
            inputs([("x", floats(&[1.5, -2.0]))]),
        );
        assert_eq!(failure, None);
        assert_eq!(rows[0][&VarName::from("s")], Value::Float(1.5));
        // The checked factory clamps.
        assert_eq!(rows[1][&VarName::from("s")], Value::Float(0.0));
        assert_eq!(
            rows[0][&VarName::from("d_out")],
            value_of(JOURNAL_DURATION, 1.5)
        );
    }
}

/// The site of the `dynamic` node the library's `read` wrote, as elaborated
/// into `y`.
fn library_site() -> RuntimeExpressionSite {
    let elaborated = elaborate(
        program("in s: Str\nout y: time::Duration<time::Local> = time::read(s)\n"),
        TypeCheckOptions::STRICT,
    );
    elaborated
        .var_expr_ref(&VarName::from("y"))
        .unwrap()
        .postorder()
        .find(|node| is_runtime_expression(node.expr()))
        .expect("a dynamic node")
        .runtime_expression()
        .clone()
}

fn refused_as_runtime_text(
    result: Result<crate::lang::dsrv::ast::CheckedExpr, RuntimeExpressionError>,
) -> bool {
    matches!(
        result,
        Err(RuntimeExpressionError::Parse {
            error: DsrvParseError::Resolve(SourceResolveError::NewtypeRuntimeText { .. }),
            ..
        })
    )
}

#[test]
fn runtime_text_never_builds_or_reads_a_representation() {
    let site = library_site();
    // Even at a site the declaring module wrote, read in its namespace.
    for text in [
        "Duration::new(1.0)",
        "Duration::new(Duration::value(seconds(1.0)))",
    ] {
        assert!(
            refused_as_runtime_text(site.parse_and_check(text)),
            "{text}"
        );
    }
    // The module's exported factory is how runtime text builds one; its
    // body, written by the module, is elaborated before it runs.
    let accepted = site
        .parse_and_check("seconds(2.0)")
        .expect("the factory checks");
    assert!(
        accepted
            .as_ref()
            .postorder()
            .any(|node| matches!(node.kind(), ExprKind::Wrap(..))),
        "{}",
        accepted.as_ref().expr()
    );
    assert!(
        accepted
            .as_ref()
            .postorder()
            .all(|node| !matches!(node.kind(), ExprKind::Newtype(..)))
    );
    // Unchecked text cannot run the factory's `new` at all.
    assert!(matches!(
        site.parse_unchecked("seconds(2.0)"),
        Err(RuntimeExpressionError::UncheckedNewtype { .. })
    ));
    // Text supplied to a site of the root module is refused too.
    let root = parse_str(
        "use experimental::{newtypes}\nnewtype Meters = Float\n\
         in s: Str\nout m: Meters = dynamic(s: Meters)\n",
    )
    .unwrap();
    let elaborated = elaborate(root, TypeCheckOptions::STRICT);
    let site = elaborated
        .var_expr_ref(&VarName::from("m"))
        .unwrap()
        .runtime_expression()
        .clone();
    assert!(refused_as_runtime_text(
        site.parse_and_check("Meters::new(1.0)")
    ));
}

/// Dataflow runs no `dynamic` inside a def's body, so the site here is
/// the root's own; its text reaches `new` through the exported factory.
#[test]
fn runtime_text_reaching_new_runs_the_library_factory_on_both_runtimes() {
    let spec = program(
        "in s: Str\n\
         out y: time::Duration<time::Local> = dynamic(s: time::Duration<time::Local>)\n",
    );
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(spec.clone(), options),
            inputs([("s", vec![Value::Str("time::local_seconds(2.0)".into())])]),
        );
        assert_eq!(failure, None);
        assert_eq!(
            rows[0][&VarName::from("y")],
            value_of("time::Duration<time::Local>", 2.0)
        );
    }
}

// -----------------------------------------------------------------------------
// AC-NT-4: `Any`, and AC-NT-7: both runtimes, failing at the same tick
// -----------------------------------------------------------------------------

/// A program in which `pick`, of type `Any`, is a journal duration until `x`
/// reaches 2, and then `other`.
fn picking(other: &str, body: &str) -> DsrvSpecification {
    program(&format!(
        "in x: Float\n\
         aux dj: DJ = time::seconds(x)\n\
         aux ij: IJ = time::at(x)\n\
         aux ip: IP = time::at(x)\n\
         aux any_d: Any = dj\n\
         aux any_i: Any = ij\n\
         aux any_p: Any = ip\n\
         aux any_raw: Any = x\n\
         aux pick: Any = if x < 2.0 then any_d else {other}\n\
         {body}"
    ))
}

/// Run a program on both runtimes over `x` = 0, 1, 2, 3, returning how many
/// ticks completed and the failure.
fn run_picking(spec: DsrvSpecification, options: TypeCheckOptions) -> (usize, Option<String>) {
    let (rows, failure) = run_both(
        elaborate(spec, options),
        inputs([("x", floats(&[0.0, 1.0, 2.0, 3.0]))]),
    );
    (rows.len(), failure)
}

#[test]
fn a_value_keeps_its_identity_through_any() {
    // Duration<JournalTime> -> Any -> Duration<JournalTime> succeeds.
    let spec = picking(
        "any_d",
        "out back: DJ = pick as DJ\nout s: Float = time::secs(back)\n",
    );
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(spec.clone(), options),
            inputs([("x", floats(&[0.0, 3.0]))]),
        );
        assert_eq!(failure, None);
        assert_eq!(
            rows[1][&VarName::from("back")],
            value_of(JOURNAL_DURATION, 3.0)
        );
        assert_eq!(rows[1][&VarName::from("s")], Value::Float(3.0));
    }
}

/// The failing N3 cases, through an explicit cast, an assignment to a
/// declared stream and a parameter: each fails with a type error at the
/// tick the wrong value arrives, the same on both runtimes, and never yields
/// `false` or no value.
#[test]
fn a_cast_or_boundary_never_manufactures_an_identity() {
    let cases = [
        // Instant<JournalTime> -> Any -> Duration<JournalTime>.
        ("any_i", "time::Instant<app::JournalTime>"),
        // A raw Float -> Any -> Duration<JournalTime>.
        ("any_raw", "an unbranded value"),
    ];
    for (other, actual) in cases {
        for body in [
            "out back: DJ = pick as DJ\n",
            // An implicit assignment boundary.
            "out back: DJ = pick\n",
            // An implicit parameter boundary, whose callee ignores the
            // argument.
            "def ignore(d: DJ) -> Float = 0.0\nout back: Float = ignore(pick)\n",
        ] {
            for options in BOTH {
                let (ticks, failure) = run_picking(picking(other, body), options);
                let failure = failure.unwrap_or_else(|| panic!("{other} {body}: no failure"));
                assert_eq!(ticks, 2, "{other} {body}: fails at the tick it arrives");
                // The failure names the value that arrived at tick 2.
                assert!(
                    failure.contains(JOURNAL_DURATION)
                        && failure.contains(actual)
                        && failure.contains("2.0"),
                    "{other} {body}: {failure}"
                );
            }
        }
    }
    // Instant<Producer> -> Any -> Instant<JournalTime>.
    let spec = program(
        "in x: Float\n\
         aux ij: IJ = time::at(x)\n\
         aux ip: IP = time::at(x)\n\
         aux any_i: Any = ij\n\
         aux any_p: Any = ip\n\
         aux pick: Any = if x < 2.0 then any_i else any_p\n\
         out back: IJ = pick as IJ\n",
    );
    for options in BOTH {
        let (ticks, failure) = run_picking(spec.clone(), options);
        let failure = failure.expect("the producer instant is refused");
        assert_eq!(ticks, 2);
        assert!(
            failure.contains("time::Instant<app::Producer>"),
            "{failure}"
        );
    }
}

#[test]
fn value_of_a_value_of_another_newtype_fails_at_that_use() {
    // Inside the declaring module, `Duration::value` of an `Any` that turns
    // out to hold an instant.
    let module = "use experimental::{modules, functions, newtypes}\n\
                  newtype Duration = Float\nnewtype Instant = Float\n\
                  def any_secs(d: Any) -> Float = Duration::value(d)\n\
                  def at(x: Float) -> Instant = Instant::new(x)\n\
                  def seconds(x: Float) -> Duration = Duration::new(x)\n";
    let root = "use experimental::{modules, functions}\nmod m\nuse m\n\
                in x: Float\n\
                aux d: Any = m::seconds(x)\naux i: Any = m::at(x)\n\
                aux pick: Any = if x < 2.0 then d else i\n\
                out s: Float = m::any_secs(pick)\n";
    let spec = expand(root, &[("m", module)]).unwrap();
    for options in BOTH {
        let (ticks, failure) = run_picking(spec.clone(), options);
        let failure = failure.expect("the instant is refused");
        assert_eq!(ticks, 2);
        assert!(
            failure.contains("m::Duration") && failure.contains("m::Instant"),
            "{failure}"
        );
    }
}

// -----------------------------------------------------------------------------
// AC-NT-6: serialisation, and unchecked decoded input
// -----------------------------------------------------------------------------

#[test]
fn a_decoded_value_on_a_newtype_input_keeps_its_identity() {
    let written = value_of(JOURNAL_DURATION, 2.5).encode_json().unwrap();
    assert_eq!(
        written,
        r#"{"$newtype":"time::Duration<app::JournalTime>","value":2.5}"#
    );
    let decoded = Value::decode_json(written.as_bytes()).unwrap();
    let spec = program("in d: DJ\nout s: Float = time::secs(d)\nout back: DJ = d\n");
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(spec.clone(), options),
            inputs([("d", vec![decoded.clone()])]),
        );
        assert_eq!(failure, None);
        assert_eq!(rows[0][&VarName::from("s")], Value::Float(2.5));
        assert_eq!(rows[0][&VarName::from("back")], decoded);
        // The output encodes as it was read.
        assert_eq!(
            rows[0][&VarName::from("back")].encode_json().unwrap(),
            written
        );
    }
}

/// Input is not checked (N8): a mismatched brand, or a bare `Float`, enters
/// as it was decoded and fails at its first incompatible use.
#[test]
fn a_mismatched_decoded_value_fails_at_its_first_use_not_at_input() {
    let good =
        Value::decode_json(br#"{"$newtype":"time::Duration<app::JournalTime>","value":1.0}"#)
            .unwrap();
    let wrong =
        Value::decode_json(br#"{"$newtype":"time::Duration<app::Producer>","value":1.0}"#).unwrap();
    for (bad, actual) in [
        (wrong, "time::Duration<app::Producer>"),
        (Value::Float(1.0), "not a newtype value"),
    ] {
        // `raw` passes the input straight through, which is not a use.
        let spec = program("in d: DJ\nout raw: DJ = d\nout s: Float = time::secs(d)\n");
        for options in BOTH {
            let (rows, failure) = run_both(
                elaborate(spec.clone(), options),
                inputs([("d", vec![good.clone(), good.clone(), bad.clone()])]),
            );
            assert_eq!(rows.len(), 2, "{bad}: accepted until it is used");
            let failure = failure.expect("the use fails");
            assert!(failure.contains(actual), "{failure}");
        }
        // Passed through without a use, it is not rejected at input.
        let spec = program("in d: DJ\nout raw: DJ = d\n");
        for options in BOTH {
            let (rows, failure) = run_both(
                elaborate(spec.clone(), options),
                inputs([("d", vec![bad.clone()])]),
            );
            assert_eq!(failure, None);
            assert_eq!(rows[0][&VarName::from("raw")], bad);
        }
    }
}

// -----------------------------------------------------------------------------
// AC-NT-7 and AC-NT-9: elaboration
// -----------------------------------------------------------------------------

#[test]
fn no_newtype_operation_survives_elaboration() {
    let spec = picking(
        "any_d",
        "out back: DJ = pick as DJ\nout s: Float = time::secs(dj)\n",
    );
    for options in BOTH {
        let elaborated = elaborate(spec.clone(), options);
        assert!(
            elaborated
                .source()
                .unchecked()
                .nodes()
                .any(|node| matches!(node.kind(), ExprKind::Newtype(..)))
        );
        let (mut wraps, mut unwraps) = (0, 0);
        for node in elaborated.checked().unchecked().nodes() {
            match node.kind() {
                ExprKind::Newtype(..) => panic!("a frontend newtype node survived: {node}"),
                // A cast into a newtype became a checked ascription.
                ExprKind::Cast(_, target) => {
                    assert!(!matches!(target, StreamType::Newtype(_)), "{node}")
                }
                ExprKind::Wrap(..) => wraps += 1,
                ExprKind::Unwrap(..) => unwraps += 1,
                _ => {}
            }
        }
        assert!(wraps > 0 && unwraps > 0);
    }
}

/// Malformed construction fails before its value is taken for a newtype's:
/// an operand of type `Any` that is not the representation is refused by
/// the representation check elaboration puts in front of the wrap.
#[test]
fn malformed_construction_fails_before_a_value_is_branded() {
    let source = "use experimental::{newtypes}\nnewtype Meters = Float\n\
                  in raw\nout m: Meters = Meters::new(raw)\n";
    let elaborated = elaborate(parse_str(source).unwrap(), TypeCheckOptions::GRADUAL);
    let (rows, failure) = run_both(
        elaborated,
        inputs([("raw", vec![Value::Float(1.0), Value::Str("far".into())])]),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][&VarName::from("m")], value_of("Meters", 1.0));
    let failure = failure.expect("a string is not a Float");
    assert!(failure.contains("cannot cast"), "{failure}");
}

#[test]
fn an_int_operand_is_promoted_to_the_representation() {
    let source = "use experimental::{newtypes}\nnewtype Meters = Float\n\
                  in n: Int\nout m: Meters = Meters::new(n)\n";
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(parse_str(source).unwrap(), options),
            inputs([("n", vec![Value::Int(3)])]),
        );
        assert_eq!(failure, None);
        assert_eq!(rows[0][&VarName::from("m")], value_of("Meters", 3.0));
    }
}

/// Absence is not a value: `new` and `value` of no value give no value.
#[test]
fn no_value_stays_absent() {
    let source = "use experimental::{newtypes}\nnewtype Meters = Float\n\
                  in x: Float\nin y: Float\n\
                  out m: Meters = Meters::new(x)\nout v: Float = Meters::value(m)\n\
                  out w: Float = y\n";
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(parse_str(source).unwrap(), options),
            inputs([
                ("x", vec![Value::NoVal, Value::Float(1.0)]),
                ("y", floats(&[1.0, 2.0])),
            ]),
        );
        assert_eq!(failure, None);
        assert_eq!(rows[0][&VarName::from("m")], Value::NoVal);
        assert_eq!(rows[0][&VarName::from("v")], Value::NoVal);
        assert_eq!(rows[1][&VarName::from("m")], value_of("Meters", 1.0));
        assert_eq!(rows[1][&VarName::from("v")], Value::Float(1.0));
    }
}

// -----------------------------------------------------------------------------
// Nested containers stay structural
// -----------------------------------------------------------------------------

#[test]
fn a_list_of_newtypes_is_structural_around_branded_elements() {
    let spec = program(
        "in x: Float\n\
         aux d: DJ = time::seconds(x)\n\
         out ds: List<DJ> = [d, d]\n\
         out first: Float = time::secs(List.get(ds, 0))\n\
         aux loose: Any = ds\n\
         out back: List<DJ> = loose\n",
    );
    assert_eq!(
        annotation(&spec, "ds"),
        StreamType::List(Box::new(annotation(&spec, "d")))
    );
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(spec.clone(), options),
            inputs([("x", floats(&[4.0]))]),
        );
        assert_eq!(failure, None);
        let element = value_of(JOURNAL_DURATION, 4.0);
        assert_eq!(
            rows[0][&VarName::from("ds")],
            Value::List(vec![element.clone(), element].into())
        );
        assert_eq!(rows[0][&VarName::from("first")], Value::Float(4.0));
    }
}

// -----------------------------------------------------------------------------
// AC-NT-8: ordinary programs are unchanged
// -----------------------------------------------------------------------------

#[test]
fn a_program_without_newtypes_elaborates_to_the_tree_it_was() {
    let source = "use experimental::{newtypes, casts}\n\
                  in x: Int\nin y: Any\nout z: Float = (x + 1) as Float\nout w: Any = y\n";
    for options in BOTH {
        let checked = crate::dsrv_fixtures::checked_with(source, options);
        let elaborated = checked.clone().elaborate();
        assert_eq!(elaborated.checked().unchecked(), checked.unchecked());
    }
}

#[test]
fn ordinary_scalar_programs_keep_the_typed_path_and_newtypes_leave_it() {
    let scalar = "use experimental::{newtypes}\nnewtype Meters = Float\n\
                  in x: Float\nout y: Float = x * 2.0\n";
    let spec = elaborate(parse_str(scalar).unwrap(), TypeCheckOptions::STRICT);
    let mut typed = TypedDataflowMonitor::<(f64,), (f64,)>::compile_checked(spec)
        .expect("a scalar program binds typed rows");
    assert_eq!(typed.evaluate(&(1.5,)), (3.0,));
    // A newtype-typed stream is not a scalar: the program takes the Value
    // path, as a union does.
    let nominal = "use experimental::{newtypes}\nnewtype Meters = Float\n\
                   in x: Float\nout y: Meters = Meters::new(x * 2.0)\n";
    let spec = elaborate(parse_str(nominal).unwrap(), TypeCheckOptions::STRICT);
    assert!(TypedDataflowMonitor::<(f64,), (f64,)>::compile_checked(spec.clone()).is_err());
    let mut monitor = DataflowMonitor::compile_checked(spec).unwrap();
    let mut output = vec![Value::NoVal];
    monitor.evaluate(&[Value::Float(1.5)], &mut output).unwrap();
    assert_eq!(output[0], value_of("Meters", 3.0));
}

#[test]
fn a_specification_with_newtypes_prints_back_as_itself() {
    let source = "use experimental::{newtypes}\nnewtype Meters = Float\n\
                  in x: Float\nout m: Meters = Meters::new(x)\nout v: Float = Meters::value(m)\n";
    let printed = parse_str(source).unwrap().to_string();
    assert!(printed.contains("newtype Meters = Float"), "{printed}");
    assert!(printed.contains("Meters::new(x)"), "{printed}");
    assert!(printed.contains("Meters::value(m)"), "{printed}");
    assert_eq!(parse_str(&printed).unwrap().to_string(), printed);
    // A value on standard output names its brand.
    assert_eq!(value_of("Meters", 1.5).dsrv_source(), "Meters::new(1.5)");
}

// N5: the brand is the declaring module's canonical path and the canonical
// resolved arguments, however the type was written.
#[test]
fn brands_are_pinned_canonical_strings() {
    let spec = program(
        "type Seconds = Float\n\
         in domain: app::JournalTime\n\
         in imported: IJ\n\
         in aliased: time::Instant<J>\n\
         in structural: time::Instant<Seconds>\n\
         in nested: time::Duration<IJ>\n\
         in container: time::Instant<List<J>>\n",
    );
    for (var, expected) in [
        ("domain", "app::JournalTime"),
        ("imported", "time::Instant<app::JournalTime>"),
        ("aliased", "time::Instant<app::JournalTime>"),
        ("structural", "time::Instant<Float>"),
        ("nested", "time::Duration<time::Instant<app::JournalTime>>"),
        ("container", "time::Instant<List<app::JournalTime>>"),
    ] {
        assert_eq!(brand(&spec, var), expected, "{var}");
    }
}

// AC-NT-5 with N4-int: runtime text under a library-authored `dynamic` sees
// the library as an importer does, so it can neither call its internal
// factory nor write `new` itself.
#[test]
fn runtime_text_neither_calls_the_owners_internals_nor_uses_new() {
    let site = library_site();
    let hidden = site.parse_and_check("raw_seconds(1.0)");
    assert!(
        matches!(
            &hidden,
            Err(RuntimeExpressionError::TypeCheck { errors, .. })
                if matches!(errors.as_slice(), [SemanticError::UndeclaredVariable(..)])
        ),
        "the internal factory is hidden: {hidden:?}"
    );
    assert!(refused_as_runtime_text(
        site.parse_and_check("Duration::new(1.0)")
    ));
    // What it exports stays callable.
    site.parse_and_check("local_seconds(1.0)")
        .expect("the exported factory checks");
}

/// `==` and `!=` never answer `false` for values of different newtypes, or
/// a newtype and a bare value: the mismatch is a type error at the tick it
/// happens, on both runtimes.
#[test]
fn comparing_across_brands_is_a_runtime_type_error() {
    for (other, actual) in [
        ("any_i", "time::Instant<app::JournalTime>"),
        ("any_raw", "not a newtype value"),
    ] {
        for operator in ["==", "!="] {
            let body = format!("out same: Bool = pick {operator} any_d\n");
            for options in BOTH {
                let (ticks, failure) = run_picking(picking(other, &body), options);
                let failure = failure.unwrap_or_else(|| panic!("{other} {operator}: no failure"));
                assert_eq!(ticks, 2, "{other} {operator}");
                assert!(
                    failure.contains("cannot compare")
                        && failure.contains(JOURNAL_DURATION)
                        && failure.contains(actual),
                    "{failure}"
                );
            }
        }
    }
    // The same newtype compares by its representation, nested or not.
    let spec = picking(
        "any_d",
        "out same: Bool = pick == any_d\nout listed: Bool = [pick] != [any_d]\n",
    );
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(spec.clone(), options),
            inputs([("x", floats(&[0.0, 3.0]))]),
        );
        assert_eq!(failure, None);
        assert_eq!(rows[1][&VarName::from("same")], Value::Bool(true));
        assert_eq!(rows[1][&VarName::from("listed")], Value::Bool(false));
    }
}

// -----------------------------------------------------------------------------
// AC-NT item 7: constants holding newtype values
// -----------------------------------------------------------------------------

/// A toy clock library: a generic newtype whose phantom parameter only its
/// factories' results mention, constants of it built by the module itself
/// and through its checked factory, and a library-authored runtime
/// expression site. It is not the planned typed timing library.
const CLOCK: &str = "\
use experimental::{modules, functions, generics, newtypes, constants}
use app
newtype Span<D> = Float
newtype Local = Unit

// The checked factory: a negative amount is clamped to zero.
def seconds<D>(x: Float) -> Span<D> =
  if x >= 0.0 then Span::new(x) else Span::new(0.0)
// A factory that calls the other.
def millis<D>(x: Float) -> Span<D> = seconds(x / 1000.0)
def secs<D>(s: Span<D>) -> Float = Span::value(s)
def longer<D>(a: Span<D>, b: Span<D>) -> Bool = Span::value(a) > Span::value(b)

// The declaring module may write `new` in a constant, even one it keeps to
// itself.
internal const raw_tick: Span<Local> = Span::new(0.5)
const tick: Span<Local> = raw_tick
const journal_budget: Span<app::JournalTime> = seconds(0.2)

// A library-authored runtime expression site.
def read(s: Str) -> Span<Local> = dynamic(s: Span<Local>)
";

const CLOCK_ROOT: &str = "\
use experimental::{modules, functions, generics, newtypes, casts, constants}
mod app
mod clock
use app
use clock
type SJ = clock::Span<app::JournalTime>
type SP = clock::Span<app::Producer>
";

const JOURNAL_SPAN: &str = "clock::Span<app::JournalTime>";

fn clock_program(body: &str) -> Result<DsrvSpecification, DsrvExpandError> {
    expand(
        &format!("{CLOCK_ROOT}{body}"),
        &[("app", APP), ("clock", CLOCK)],
    )
}

fn clock_spec(body: &str) -> DsrvSpecification {
    clock_program(body).unwrap_or_else(|error| panic!("{body}: {error}"))
}

/// The literal `var`'s equation is, which is what a constant folded to.
fn folded(spec: &DsrvSpecification, var: &str) -> crate::lang::dsrv::ast::SyntaxLiteral {
    match spec.var_expr_ref(&VarName::from(var)).unwrap().kind() {
        ExprKind::Val(value) => value.clone(),
        other => panic!("{var} is not a literal: {other:?}"),
    }
}

/// The brand N5 gives a span in the toy library's `app` domain `domain`.
fn span_brand(domain: &str) -> ecow::EcoString {
    let journal = StreamType::Newtype(crate::core::NewtypeSchema::new(
        format!("app::{domain}"),
        ecow::EcoVec::new(),
        StreamType::Unit,
    ));
    crate::core::canonical_brand("clock::Span", &[journal])
}

// Criterion 1: a constant calling an exported factory folds to a newtype
// value with its canonical brand, and the phantom parameter, which only the
// factory's result mentions, is inferred from the constant's declared type.
#[test]
fn a_constant_folds_through_an_exported_factory_to_a_branded_value() {
    use crate::lang::dsrv::ast::SyntaxLiteral;
    let spec = clock_spec(
        "const budget: SJ = clock::seconds(0.2)\n\
         // A factory whose body calls another.\n\
         const short: SJ = clock::millis(50.0)\n\
         // Nested calls, and another constant read through the factory's\n\
         // accessor.\n\
         const twice: SJ = clock::seconds(clock::secs(budget) * 2.0)\n\
         // The checked factory clamps here as it does at run time.\n\
         const clamped: SP = clock::seconds(0.0 - 1.0)\n\
         out b: SJ = budget\n\
         out s: SJ = short\n\
         out t: SJ = twice\n\
         out c: SP = clamped\n",
    );
    assert_eq!(span_brand("JournalTime"), JOURNAL_SPAN);
    for (var, brand, expected) in [
        ("b", span_brand("JournalTime"), 0.2),
        ("s", span_brand("JournalTime"), 0.05),
        ("t", span_brand("JournalTime"), 0.4),
        ("c", span_brand("Producer"), 0.0),
    ] {
        let SyntaxLiteral::Newtype(schema, value) = folded(&spec, var) else {
            panic!("{var} did not fold to a newtype value");
        };
        assert_eq!(*schema.brand(), brand, "{var}");
        assert_eq!(*value, SyntaxLiteral::Float(expected), "{var}");
        assert_eq!(schema.repr(), &StreamType::Float);
        // The value is checked as that instance where it is used.
        assert_eq!(
            annotation(&spec, var),
            StreamType::Newtype(schema.clone()),
            "{var}"
        );
    }
    for options in BOTH {
        let elaborated = elaborate(spec.clone(), options);
        assert_eq!(
            elaborated
                .var_expr_ref(&VarName::from("b"))
                .unwrap()
                .typ()
                .to_string(),
            JOURNAL_SPAN
        );
    }
}

#[test]
fn a_constant_of_newtype_type_is_checked_when_it_is_folded() {
    // The declared type is what the body is checked against, so a newtype
    // is not a Float, and another domain is not this one.
    for body in [
        "const bad: Float = clock::seconds(0.2)\n",
        "const bad: SP = clock::journal_budget\n",
        "const bad: Bool = clock::longer(clock::tick, clock::journal_budget)\n",
    ] {
        let error = clock_program(body).expect_err(body);
        assert!(
            matches!(
                &error,
                DsrvExpandError::ConstantCheck { name, errors, .. }
                    if name == "bad" && is_type_error(errors)
            ),
            "{body}: {error}"
        );
    }
    // A newtype has no arithmetic, so a constant cannot compute with one.
    let error = clock_program("const bad: SJ = clock::journal_budget + clock::journal_budget\n")
        .expect_err("no arithmetic");
    assert!(
        matches!(error, DsrvExpandError::ConstantCheck { .. }),
        "{error}"
    );
    // Nothing gives the phantom parameter a domain.
    let error = clock_program("const bad: Float = clock::secs(clock::seconds(0.2))\n")
        .expect_err("ungrounded");
    assert!(
        matches!(&error, DsrvExpandError::ConstantCheck { errors, .. }
            if format!("{errors:?}").contains("annotate")),
        "{error}"
    );
}

// Criterion 2: a constant keeps its brand when it is imported, and using it
// where another brand or its representation is expected is a type error
// when the program is checked.
#[test]
fn an_imported_constant_keeps_its_brand() {
    let spec = clock_spec(
        "out b: SJ = clock::journal_budget\n\
         out l: clock::Span<clock::Local> = clock::tick\n",
    );
    assert_eq!(brand(&spec, "b"), JOURNAL_SPAN);
    assert_eq!(brand(&spec, "l"), "clock::Span<clock::Local>");
    for options in BOTH {
        elaborate(spec.clone(), options);
    }
    // Through a glob, and through a constant of the importer.
    let root = format!(
        "{CLOCK_ROOT}use clock::*\n\
         const mine: SJ = journal_budget\n\
         out b: SJ = mine\n"
    );
    let spec = expand(&root, &[("app", APP), ("clock", CLOCK)]).unwrap();
    assert_eq!(
        folded(&spec, "b").to_string(),
        format!("{JOURNAL_SPAN}::new(0.2)")
    );
    for body in [
        "out f: Float = clock::journal_budget\n",
        "out p: SP = clock::journal_budget\n",
        "in x: Float\nout over: Bool = clock::longer(clock::journal_budget, clock::tick)\n",
        "out same: Bool = clock::journal_budget == 0.2\n",
    ] {
        for options in BOTH {
            let errors = check_errors(clock_spec(body), options);
            assert!(is_type_error(&errors), "{body}: {errors:?}");
        }
    }
}

// Criterion 2, AC-NT-4: `Any` keeps a constant's identity, and a cast only
// checks it.
#[test]
fn an_imported_constant_keeps_its_identity_through_any() {
    let spec = clock_spec(
        "in x: Float\n\
         aux any_budget: Any = clock::journal_budget\n\
         aux producer: SP = clock::seconds(x)\n\
         aux any_producer: Any = producer\n\
         aux pick: Any = if x < 1.0 then any_producer else any_budget\n\
         out back: SJ = any_budget as SJ\n\
         out wrong: SP = pick as SP\n",
    );
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(spec.clone(), options),
            inputs([("x", floats(&[0.5, 2.0]))]),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][&VarName::from("back")], value_of(JOURNAL_SPAN, 0.2));
        let failure = failure.expect("the cast to another domain fails");
        assert!(
            failure.contains("cannot cast") && failure.contains(JOURNAL_SPAN),
            "{failure}"
        );
    }
}

// Criterion 3: ownership. Only the declaring module writes `new` or `value`
// in a constant, as in an equation; the module itself may, even in one it
// keeps internal.
#[test]
fn only_the_declaring_module_writes_new_or_value_in_a_constant() {
    for body in [
        "const forged: SJ = clock::Span::new(0.2)\n",
        "const read: Float = clock::Span::value(clock::journal_budget)\n",
        "const forged: SJ = SJ::new(0.2)\n",
    ] {
        assert!(
            matches!(
                clock_program(body),
                Err(DsrvExpandError::Resolve(
                    SourceResolveError::NewtypeRepresentation { .. }
                ))
            ),
            "{body}"
        );
    }
    // The internal constant that wrote `new` gave the exported one its
    // value.
    let spec = clock_spec("out l: clock::Span<clock::Local> = clock::tick\n");
    assert_eq!(
        folded(&spec, "l").to_string(),
        "clock::Span<clock::Local>::new(0.5)"
    );
    // A root module's own newtype may be built by its own constant.
    let source = "use experimental::{newtypes, constants}\n\
                  newtype Meters = Float\n\
                  const metre: Meters = Meters::new(1.0)\n\
                  const back: Float = Meters::value(metre)\n\
                  out m: Meters = metre\nout v: Float = back\n";
    let spec = parse_str(source).unwrap();
    assert_eq!(folded(&spec, "m").to_string(), "Meters::new(1.0)");
    assert_eq!(folded(&spec, "v").to_string(), "1.0");
    for options in BOTH {
        elaborate(spec.clone(), options);
    }
}

/// The site of the `dynamic` node the clock library's `read` wrote.
fn clock_site() -> RuntimeExpressionSite {
    let elaborated = elaborate(
        clock_spec("in s: Str\nout y: clock::Span<clock::Local> = clock::read(s)\n"),
        TypeCheckOptions::STRICT,
    );
    elaborated
        .var_expr_ref(&VarName::from("y"))
        .unwrap()
        .postorder()
        .find(|node| is_runtime_expression(node.expr()))
        .expect("a dynamic node")
        .runtime_expression()
        .clone()
}

// Criterion 4: N4-int is unchanged. Runtime text sees a module's constants
// as an importer does: an exported constant of newtype type, with its
// brand, and never an internal one.
#[test]
fn runtime_text_sees_only_exported_newtype_constants() {
    let site = clock_site();
    let accepted = site
        .parse_and_check("tick")
        .expect("an exported constant is visible");
    assert!(
        matches!(accepted.as_ref().expr().kind(), ExprKind::Val(value)
            if value.to_string() == "clock::Span<clock::Local>::new(0.5)"),
        "{}",
        accepted.as_ref().expr()
    );
    let hidden = site.parse_and_check("raw_tick");
    assert!(
        matches!(
            &hidden,
            Err(RuntimeExpressionError::TypeCheck { errors, .. })
                if matches!(errors.as_slice(), [SemanticError::UndeclaredVariable(..)])
        ),
        "the internal constant is hidden: {hidden:?}"
    );
    // The brand still decides where the value may go.
    assert!(matches!(
        site.parse_and_check("journal_budget"),
        Err(RuntimeExpressionError::TypeCheck { .. })
    ));
    // At a site the root wrote, text reaches the constant through its
    // module, and a run gives the value its brand on both runtimes.
    let spec = clock_spec(
        "in s: Str\n\
         out y: clock::Span<clock::Local> = dynamic(s: clock::Span<clock::Local>)\n",
    );
    for options in BOTH {
        let (rows, failure) = run_both(
            elaborate(spec.clone(), options),
            inputs([("s", vec![Value::Str("clock::tick".into())])]),
        );
        assert_eq!(failure, None);
        assert_eq!(
            rows[0][&VarName::from("y")],
            value_of("clock::Span<clock::Local>", 0.5)
        );
    }
}

/// The asynchronous runtime publishes each output as it is computed rather
/// than a row per tick, so its rows are regrouped: the n-th value of each
/// output belongs to tick n.
fn by_tick(rows: Vec<BTreeMap<VarName, Value>>) -> Vec<BTreeMap<VarName, Value>> {
    let mut ticks: Vec<BTreeMap<VarName, Value>> = Vec::new();
    let mut seen: BTreeMap<VarName, usize> = BTreeMap::new();
    for (var, value) in rows.into_iter().flatten() {
        let tick = seen.entry(var.clone()).or_default();
        if ticks.len() <= *tick {
            ticks.push(BTreeMap::new());
        }
        ticks[*tick].insert(var, value);
        *tick += 1;
    }
    ticks
}

// Criterion 5: a newtype constant in stream expressions behaves the same on
// the dataflow, semi-sync and asynchronous runtimes.
#[test]
fn a_newtype_constant_runs_the_same_on_every_runtime() {
    let spec = clock_spec(
        "const budget: SJ = clock::seconds(0.2)\n\
         in x: Float\n\
         aux elapsed: SJ = clock::seconds(x)\n\
         out over: Bool = clock::longer(elapsed, budget)\n\
         out left: Float = clock::secs(budget) - x\n\
         out longest: SJ = if clock::longer(elapsed, budget) then elapsed else budget\n\
         out spans: List<SJ> = [budget, elapsed]\n",
    );
    let xs = [0.1, 0.3, -1.0];
    for options in BOTH {
        let elaborated = elaborate(spec.clone(), options);
        let given = inputs([("x", floats(&xs))]);
        let (rows, failure) = run_both(elaborated.clone(), given.clone());
        assert_eq!(failure, None);
        let (async_rows, async_failure) = run(
            RuntimeSpec::Async,
            semantics_for(&elaborated),
            elaborated,
            given,
        );
        assert_eq!(async_failure, None);
        assert_eq!(
            by_tick(async_rows),
            rows,
            "the asynchronous runtime disagrees"
        );
        assert_eq!(rows.len(), xs.len());
        let at = |tick: usize, var: &str| rows[tick][&VarName::from(var)].clone();
        assert_eq!(at(0, "over"), Value::Bool(false));
        assert_eq!(at(1, "over"), Value::Bool(true));
        // The factory clamped the negative reading to zero.
        assert_eq!(at(2, "over"), Value::Bool(false));
        assert_eq!(at(2, "left"), Value::Float(1.2));
        assert_eq!(at(0, "longest"), value_of(JOURNAL_SPAN, 0.2));
        assert_eq!(at(1, "longest"), value_of(JOURNAL_SPAN, 0.3));
        assert_eq!(
            at(1, "spans"),
            Value::List(vec![value_of(JOURNAL_SPAN, 0.2), value_of(JOURNAL_SPAN, 0.3)].into())
        );
    }
}

// Criterion 6: programs without newtypes are unchanged by folding through
// calls, and still take the typed scalar path.
#[test]
fn a_constant_calling_an_ordinary_def_folds_and_keeps_the_typed_path() {
    let source = "use experimental::{functions, constants}\n\
                  def twice(n: Int) -> Int = n * 2\n\
                  def half(x: Float) -> Float = x / 2.0\n\
                  const lag: Int = twice(1)\n\
                  const scale: Float = half(3.0) + 0.5\n\
                  in x: Float\n\
                  out y: Float = default(x[lag], 0.0) * scale\n";
    let spec = parse_str(source).unwrap();
    let printed = spec.var_expr_ref(&VarName::from("y")).unwrap().to_string();
    assert!(
        printed.contains("x[2]") && printed.contains("2.0") && !printed.contains("scale"),
        "{printed}"
    );
    let elaborated = elaborate(spec, TypeCheckOptions::STRICT);
    let mut typed = TypedDataflowMonitor::<(f64,), (f64,)>::compile_checked(elaborated)
        .expect("a scalar program binds typed rows");
    assert_eq!(typed.evaluate(&(1.0,)), (0.0,));
    assert_eq!(typed.evaluate(&(2.0,)), (0.0,));
    assert_eq!(typed.evaluate(&(3.0,)), (2.0,));
}

// Criterion 6: a constant that cannot be folded says why, when the program
// is read, and never panics.
#[test]
fn a_constant_that_cannot_be_folded_is_refused_with_a_reason() {
    let header = "use experimental::{functions, constants}\n\
                  def twice(n: Int) -> Int = n * 2\n\
                  def previous(s: Int) -> Int = default(s[1], 0)\n\
                  def defer_it(s: Str) -> Int = dynamic(s: Int)\n\
                  in x: Int\n";
    for (body, expected) in [
        // It reads a stream.
        ("const c: Int = twice(x)\n", "`x` names no constant"),
        // It calls a def that reads history, or one that runs text.
        (
            "const c: Int = previous(1)\n",
            "cannot be built from a stream offset",
        ),
        (
            "const c: Int = defer_it(\"1\")\n",
            "cannot be built from text supplied at runtime",
        ),
        // It calls nothing that exists.
        ("const c: Int = thrice(1)\n", "`thrice` names no constant"),
        // Its value fails.
        ("const c: Int = twice(1) / 0\n", "could not be worked out"),
        // It is defined, through a def, in terms of itself.
        (
            "def again(n: Int) -> Int = n + c\nconst c: Int = again(1)\n",
            "in terms of itself",
        ),
        // A function is not a value a constant can hold.
        (
            "const c: Int = (\\n: Int -> n)\n",
            "cannot be built from a function",
        ),
    ] {
        let source = format!("{header}{body}out y: Int = c\n");
        let message = match parse_str(&source) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("{body}: expected a refusal"),
        };
        assert!(message.contains(expected), "{body}: {message}");
    }
    // A def parameter that shares a constant's name is not that constant.
    let source = format!(
        "{header}def plus(c: Int) -> Int = c + 1\nconst c: Int = plus(1)\nout y: Int = c\n"
    );
    assert_eq!(folded(&parse_str(&source).unwrap(), "y").to_string(), "2");
}
