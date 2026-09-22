//! Compact provenance, owned findings and their release.
//!
//! Fixtures give every file a distinct label and distinct text, so a site
//! resolved against the wrong file shows up as the wrong snippet.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};

use contiguous_tree::TreeCursorExt;

use super::*;
use crate::core::StreamType;
use crate::lang::dsrv::TypeCheckOptions;
use crate::lang::dsrv::ast::{AstShared, CheckedDsrvSpecification, DsrvSpecification, Expr};
use crate::lang::dsrv::diagnostics::{SemanticError, SemanticWarning};
use crate::lang::dsrv::expand::language::LanguageRequest;
use crate::lang::dsrv::modules::ModuleCollector;
use crate::lang::dsrv::parser::{DsrvParseError, parse_expr, parse_expr_with_functions};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::runtime_expression::RuntimeExpressionSite;
use crate::lang::dsrv::type_checker::type_check_expression;
use crate::{VarName, dsrv_fixtures::WithoutWarnings};

const HEADER: &str = "use experimental::{modules, functions}\n";

fn path(label: &str) -> SourceLabel {
    SourceLabel::Path(label.into())
}

/// Expand a program whose root is `root.dsrv` and whose modules are
/// supplied from `modules` as `(module, text)`, each labelled `<module>.dsrv`.
fn program(root: &str, modules: &[(&str, &str)]) -> DsrvSpecification {
    program_labelled(root, "root.dsrv", modules, "")
}

/// As [`program`], with every module label given `prefix`.
fn program_labelled(
    root: &str,
    root_label: &str,
    modules: &[(&str, &str)],
    prefix: &str,
) -> DsrvSpecification {
    let mut collector =
        ModuleCollector::with_label(root, path(root_label)).expect("a parsable root");
    while let Some(module) = collector.next_request().map(<[ModuleName]>::to_vec) {
        let wanted = crate::lang::dsrv::modules::show_path(&module);
        let text = modules
            .iter()
            .find(|(name, _)| *name == wanted)
            .unwrap_or_else(|| panic!("no source for {wanted}"))
            .1;
        collector
            .supply_labelled(text, path(&format!("{prefix}{wanted}.dsrv")))
            .expect("a parsable module");
    }
    crate::lang::dsrv::expand::expand_program(
        collector.finish().expect("collected"),
        LanguageRequest::default(),
    )
    .expect("expands")
}

/// A weak handle on the archived file labelled `label`.
fn file_probe(spec: &DsrvSpecification, label: &str) -> Weak<SourceFile> {
    let (_, file) = spec
        .sources()
        .files()
        .find(|(_, file)| file.label() == &path(label))
        .unwrap_or_else(|| panic!("no file {label}"));
    Arc::downgrade(file)
}

fn file(spec: &DsrvSpecification, label: &str) -> Arc<SourceFile> {
    file_probe(spec, label)
        .upgrade()
        .expect("held by the archive")
}

/// A located site's label and the text its span covers.
fn named(site: Option<&OwnedSite>) -> Option<(String, String)> {
    site.map(|site| {
        (
            site.label().to_string(),
            site.snippet().expect("a span within its file").to_owned(),
        )
    })
}

fn primary_and_definition(
    location: &SourceLocation,
) -> (Option<(String, String)>, Option<(String, String)>) {
    (named(location.primary()), named(location.definition()))
}

fn at(label: &str, text: &str) -> Option<(String, String)> {
    Some((label.to_owned(), text.to_owned()))
}

// The library writes a warning fixture and a type error, each in a def.
const LIB: &str = "use experimental::{modules, functions}\n\
    def tagged(n: Int) -> Str = \"warn:alpha\"\n\
    def bad(n: Int) -> Int = n + true\n\
    def good(n: Int) -> Int = n * 2\n";

// -----------------------------------------------------------------------------
// The archive
// -----------------------------------------------------------------------------

fn text_file(label: &str, text: &str) -> SourceFile {
    SourceFile::new(SourceLabel::Supplied(label.into()), Vec::new(), text)
}

#[test]
fn an_archive_addresses_its_files_by_local_id() {
    let mut archive = SourceArchive::new();
    assert_eq!(archive.root(), None);
    let root = archive.push(text_file("a", "alpha"));
    let other = archive.push(text_file("b", "beta"));
    assert_eq!(archive.root(), Some(root));
    assert_eq!(archive.file(root).unwrap().text(), "alpha");
    assert_eq!(archive.file(other).unwrap().text(), "beta");
    let owned = archive.resolve(SourceSite::new(other, Span::new(1, 3)));
    assert_eq!(owned.snippet(), Some("et"));
    assert_eq!(owned.label(), &SourceLabel::Supplied("b".into()));
}

#[test]
fn merging_keeps_the_left_ids_and_remaps_the_right() {
    let mut left = SourceArchive::new();
    let first = left.push(text_file("left", "L"));
    let mut right = SourceArchive::new();
    let right_first = right.push(text_file("right", "R"));
    let right_second = right.push(text_file("right2", "RR"));

    let (merged, remap) = left.merge(&right);
    assert_eq!(merged.file(first).unwrap().text(), "L");
    assert_eq!(
        merged.file(remap.id(right_first).unwrap()).unwrap().text(),
        "R"
    );
    assert_eq!(
        merged.file(remap.id(right_second).unwrap()).unwrap().text(),
        "RR"
    );
    assert_ne!(remap.id(right_first).unwrap(), first);
    // The result addresses the left archive's IDs as the left did, but not
    // the right's, which were renumbered.
    assert!(merged.resolves(left.token()));
    assert!(merged.resolves(merged.token()));
    assert!(!merged.resolves(right.token()));
    assert!(!left.resolves(merged.token()));
}

#[test]
fn a_remap_refuses_an_id_the_merged_archive_did_not_hold() {
    let left = SourceArchive::new();
    let (right, _) = SourceArchive::single(text_file("right", "R"));
    let (_, remap) = left.merge(&right);
    let mut foreign = SourceArchive::new();
    foreign.push(text_file("x", "x"));
    let foreign_id = foreign.push(text_file("y", "y"));
    assert_eq!(remap.id(foreign_id), Err(ProvenanceError::ForeignArchive));
}

#[test]
fn merging_an_archive_holding_the_same_file_keeps_it_once() {
    let (left, id) = SourceArchive::single(text_file("shared", "S"));
    let capture = left.capture([id]);
    let (merged, remap) = left.merge(&capture);
    assert_eq!(remap.id(id), Ok(id));
    assert_eq!(merged.held(), 1);
}

#[test]
fn a_capture_keeps_ids_and_releases_every_other_file() {
    let mut archive = SourceArchive::new();
    let kept = archive.push(text_file("kept", "K"));
    let released = archive.push(text_file("released", "R"));
    let probe = Arc::downgrade(archive.file(released).unwrap());
    let capture = archive.capture([kept]);
    drop(archive);
    assert!(probe.upgrade().is_none(), "an uncaptured file is released");
    assert_eq!(capture.file(kept).unwrap().text(), "K");
    assert!(capture.file(released).is_none());
    assert_eq!(capture.held(), 1);
}

#[test]
fn labels_name_filesystem_embedded_and_supplied_sources() {
    assert_eq!(path("lib/option.dsrv").to_string(), "lib/option.dsrv");
    assert_eq!(
        SourceLabel::Embedded("std/option.dsrv".into()).to_string(),
        "<embedded std/option.dsrv>"
    );
    assert_eq!(
        SourceLabel::Supplied(RUNTIME_EXPRESSION_LABEL.into()).to_string(),
        "<runtime expression>"
    );
    let (archive, id) = SourceArchive::single(SourceFile::new(
        SourceLabel::Embedded("std/option.dsrv".into()),
        vec![
            ModuleName::new("std").unwrap(),
            ModuleName::new("option").unwrap(),
        ],
        "def x() -> Int = 1",
    ));
    let owned = archive.resolve(SourceSite::new(id, Span::new(4, 5)));
    assert_eq!(format!("{owned:?}"), "<embedded std/option.dsrv>@4..5");
    assert_eq!(owned.file().module().len(), 2);
}

#[test]
fn positions_count_lines_and_scalars_and_refuse_spans_outside_the_text() {
    let text = "é\nab\n";
    assert_eq!(position(text, Span::new(0, 0)), Some((1, 1)));
    assert_eq!(position(text, Span::new(3, 4)), Some((2, 1)));
    assert_eq!(position(text, Span::new(4, 5)), Some((2, 2)));
    // The empty span at the very end of the text.
    assert_eq!(position(text, Span::new(6, 6)), Some((3, 1)));
    // Inside `é`, reversed and past the end.
    assert_eq!(position(text, Span::new(1, 2)), None);
    assert_eq!(position(text, Span::new(4, 3)), None);
    assert_eq!(position(text, Span::new(6, 60)), None);
}

#[test]
fn locations_are_presentation_rather_than_meaning() {
    let (archive, id) = SourceArchive::single(text_file("a", "abc"));
    let located = archive.locate(NodeOrigin::new(Some(id), None), Span::new(0, 1));
    assert_eq!(located, SourceLocation::default());
    let mut pending = SourceLocation::pending(NodeOrigin::new(Some(id), None), Span::new(0, 1));
    // Without the archive its IDs belong to, a finding stays unlocated
    // rather than resolving them anywhere else.
    pending.materialise(None);
    assert!(pending.primary().is_none());
    assert_eq!(format!("{pending:?}"), "unlocated");
}

// -----------------------------------------------------------------------------
// Attribution of findings
// -----------------------------------------------------------------------------

fn warnings_of(spec: DsrvSpecification) -> Vec<SemanticWarning> {
    let (result, warnings) = spec.check(TypeCheckOptions::STRICT).into_parts();
    result.expect("the program checks");
    warnings
}

fn errors_of(spec: DsrvSpecification) -> Vec<SemanticError> {
    spec.check(TypeCheckOptions::STRICT)
        .discard_warnings()
        .expect_err("the program does not check")
}

#[test]
fn a_finding_inlined_from_a_module_is_primary_at_the_call_with_a_definition_note() {
    let warnings = warnings_of(program(
        &format!("{HEADER}mod lib\nuse lib\nin x: Int\nout y: Str\ny = lib::tagged(x)\n"),
        &[("lib", LIB)],
    ));
    assert_eq!(warnings.len(), 1);
    assert_eq!(
        primary_and_definition(warnings[0].location()),
        (
            at("root.dsrv", "lib::tagged(x)"),
            at("lib.dsrv", "\"warn:alpha\"")
        )
    );
    assert_eq!(
        warnings[0].span(),
        warnings[0].location().primary().map(OwnedSite::span)
    );

    let errors = errors_of(program(
        &format!("{HEADER}mod lib\nuse lib\nin x: Int\nout y: Int\ny = lib::bad(x)\n"),
        &[("lib", LIB)],
    ));
    let (primary, definition) = primary_and_definition(errors[0].location());
    assert_eq!(primary, at("root.dsrv", "lib::bad(x)"));
    let (label, text) = definition.expect("a definition note");
    assert_eq!(label, "lib.dsrv");
    assert!("n + true".contains(&text), "defined at {text:?}");
}

#[test]
fn a_finding_in_an_argument_stays_with_the_caller() {
    let warnings = warnings_of(program(
        &format!(
            "{HEADER}mod lib\nuse lib\nin x: Int\nout y: Int\n\
             y = lib::good(if \"warn:alpha\" == \"\" then x else x)\n"
        ),
        &[("lib", LIB)],
    ));
    assert_eq!(
        primary_and_definition(warnings[0].location()),
        (at("root.dsrv", "\"warn:alpha\""), None)
    );
}

#[test]
fn each_call_site_is_primary_for_its_own_copy() {
    let warnings = warnings_of(program(
        &format!(
            "{HEADER}mod lib\nuse lib\nin x: Int\nout y: Str\nout z: Str\n\
             y = lib::tagged(x)\nz = lib::tagged(x + 1)\n"
        ),
        &[("lib", LIB)],
    ));
    let sites = warnings
        .iter()
        .map(|warning| primary_and_definition(warning.location()))
        .collect::<Vec<_>>();
    assert_eq!(
        sites,
        [
            (
                at("root.dsrv", "lib::tagged(x)"),
                at("lib.dsrv", "\"warn:alpha\"")
            ),
            (
                at("root.dsrv", "lib::tagged(x + 1)"),
                at("lib.dsrv", "\"warn:alpha\"")
            ),
        ]
    );
}

#[test]
fn code_reached_through_several_modules_names_the_module_that_wrote_it() {
    let warnings = warnings_of(program(
        &format!("{HEADER}mod mid\nuse mid\nin x: Int\nout y: Str\ny = mid::relay(x)\n"),
        &[
            (
                "mid",
                "use experimental::{modules, functions}\nmod leaf\nuse mid::leaf\n\
                 def relay(n: Int) -> Str = mid::leaf::tagged(n)\n",
            ),
            (
                "mid::leaf",
                "use experimental::{modules, functions}\n\
                 def tagged(n: Int) -> Str = \"warn:alpha\"\n",
            ),
        ],
    ));
    assert_eq!(
        primary_and_definition(warnings[0].location()),
        (
            at("root.dsrv", "mid::relay(x)"),
            at("mid::leaf.dsrv", "\"warn:alpha\"")
        )
    );
}

#[test]
fn a_def_of_the_root_file_is_located_in_that_file_without_a_note() {
    let warnings = warnings_of(
        format!(
            "{HEADER}def tagged(n: Int) -> Str = \"warn:alpha\"\nin x: Int\nout y: Str\ny = tagged(x)\n"
        )
        .parse::<DsrvSpecification>()
        .unwrap(),
    );
    assert_eq!(
        primary_and_definition(warnings[0].location()),
        (at(STRING_LABEL, "\"warn:alpha\""), None)
    );
}

#[test]
fn declaration_findings_are_located_in_the_root_file() {
    let errors = errors_of(
        "in x: Int\nin x: Int\nout y: Int\ny = x"
            .parse::<DsrvSpecification>()
            .unwrap(),
    );
    let SemanticError::DuplicateDeclaration { duplicate, .. } = &errors[0] else {
        panic!("expected a duplicate declaration, got {errors:?}");
    };
    let primary = errors[0].location().primary().expect("located");
    assert_eq!(primary.span(), *duplicate);
    assert_eq!(primary.snippet(), Some("in x: Int"));
}

#[test]
fn validation_findings_are_located_too() {
    let errors = "in x: Int\nout y: Int\ny = missing + x"
        .parse::<DsrvSpecification>()
        .unwrap()
        .validate()
        .expect_err("an undeclared variable");
    assert_eq!(
        named(errors[0].location().primary()),
        at(STRING_LABEL, "missing")
    );
}

// -----------------------------------------------------------------------------
// Unlocated trees
// -----------------------------------------------------------------------------

#[test]
fn a_bare_expression_is_checked_without_a_location() {
    let expression = parse_expr("1 + true").unwrap();
    assert!(
        expression
            .as_ref()
            .postorder()
            .all(|node| node.origin().is_unlocated())
    );
    let errors = type_check_expression(&expression, &StreamType::Int, &BTreeMap::new())
        .discard_warnings()
        .expect_err("ill-typed");
    assert!(errors[0].span().is_some(), "the span is kept");
    assert!(errors[0].location().primary().is_none());
}

#[test]
fn a_programmatic_specification_is_unlocated() {
    let y = VarName::new("y");
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([y.clone()]),
        BTreeMap::from([(y.clone(), Expr::Var(VarName::new("missing")))]),
        BTreeMap::from([(y, StreamType::Int)]),
        BTreeSet::new(),
    );
    let errors = errors_of(spec);
    assert!(errors[0].location().primary().is_none());
    assert!(errors[0].location().definition().is_none());
}

#[test]
fn copying_an_archive_bound_root_requires_explicit_provenance_remapping() {
    let parsed = program(
        &format!("{HEADER}mod lib\nuse lib\nin x: Int\nout y: Int\ny = lib::bad(x)\n"),
        &[("lib", LIB)],
    );
    let y = VarName::new("y");
    let root = parsed.var_expr(&y).unwrap();
    assert!(
        root.as_ref()
            .postorder()
            .any(|node| !node.origin().is_unlocated())
    );
    let error = DsrvSpecification::try_new(
        parsed.input_vars().clone(),
        parsed.output_vars().clone(),
        BTreeMap::from([(y.clone(), root)]),
        parsed.type_annotations().clone(),
        BTreeSet::new(),
    )
    .expect_err("archive-bound definitions cannot be grafted without remapping");
    assert_eq!(error, ProvenanceError::ForeignArchive);
}

#[test]
fn defs_of_one_program_cannot_be_grafted_into_text_located_in_another() {
    let spec = program(
        &format!("{HEADER}mod lib\nuse lib::*\nin x: Int\nout y: Int\ny = good(x)\n"),
        &[("lib", LIB)],
    );
    let node = spec.var_expr_ref(&VarName::new("y")).unwrap();
    let context = node.metadata().context.clone().unwrap();
    let callable = node.metadata().callable.clone().unwrap();
    let (unrelated, text) = SourceArchive::single(text_file("elsewhere", "good(1)"));
    let refused = parse_expr_with_functions(
        "good(1)",
        AstShared::clone(&context),
        AstShared::clone(&callable),
        Some((&unrelated, text)),
    );
    assert!(
        matches!(
            refused,
            Err(DsrvParseError::Provenance(ProvenanceError::ForeignArchive))
        ),
        "got {refused:?}"
    );
    // Merged onto the program's archive, the same text is accepted, and an
    // explicitly unlocated expansion keeps no provenance at all.
    let (merged, remap) = spec.sources().merge(&unrelated);
    let located = parse_expr_with_functions(
        "good(1)",
        AstShared::clone(&context),
        AstShared::clone(&callable),
        Some((&merged, remap.id(text).unwrap())),
    )
    .expect("a merged archive addresses the program's IDs");
    assert!(
        located
            .as_ref()
            .postorder()
            .any(|node| node.origin().definition.is_some())
    );
    let unlocated = parse_expr_with_functions("good(1)", context, callable, None).unwrap();
    assert!(
        unlocated
            .as_ref()
            .postorder()
            .all(|node| node.origin().is_unlocated())
    );
}

// -----------------------------------------------------------------------------
// Ownership and release
// -----------------------------------------------------------------------------

const WARNED_AND_FAILED: &str = "use experimental::{modules, functions}\nmod lib\nmod other\nuse lib\n\
    in x: Int\nout y: Str\nout z: Int\ny = lib::tagged(x)\nz = lib::bad(x)\n";

const OTHER: &str = "use experimental::{modules, functions}\ndef unused(n: Int) -> Int = n\n";

#[test]
fn a_failed_report_keeps_its_files_after_the_program_and_either_part_is_dropped() {
    let spec = program(WARNED_AND_FAILED, &[("lib", LIB), ("other", OTHER)]);
    let root = file_probe(&spec, "root.dsrv");
    let lib = file_probe(&spec, "lib.dsrv");
    let other = file_probe(&spec, "other.dsrv");
    let report = spec.check(TypeCheckOptions::STRICT);
    let (result, warnings) = report.into_parts();
    let errors = result.expect_err("bad does not check");
    assert!(
        other.upgrade().is_none(),
        "a file no finding names is released with the program"
    );

    drop(warnings);
    assert_eq!(
        primary_and_definition(errors[0].location()).0,
        at("root.dsrv", "lib::bad(x)")
    );
    assert!(root.upgrade().is_some() && lib.upgrade().is_some());
    drop(errors);
    assert!(root.upgrade().is_none() && lib.upgrade().is_none());
}

#[test]
fn the_warnings_of_a_failed_report_render_after_its_errors_are_dropped() {
    let spec = program(WARNED_AND_FAILED, &[("lib", LIB), ("other", OTHER)]);
    let lib = file_probe(&spec, "lib.dsrv");
    let (result, warnings) = spec.check(TypeCheckOptions::STRICT).into_parts();
    drop(result);
    assert_eq!(
        primary_and_definition(warnings[0].location()),
        (
            at("root.dsrv", "lib::tagged(x)"),
            at("lib.dsrv", "\"warn:alpha\"")
        )
    );
    drop(warnings);
    assert!(lib.upgrade().is_none());
}

#[test]
fn a_successful_report_keeps_its_warnings_files_after_the_checked_program_is_dropped() {
    let spec = program(
        &format!("{HEADER}mod lib\nuse lib\nin x: Int\nout y: Str\ny = lib::tagged(x)\n"),
        &[("lib", LIB)],
    );
    let lib = file_probe(&spec, "lib.dsrv");
    let (checked, warnings) = spec
        .check_and_elaborate(TypeCheckOptions::GRADUAL)
        .into_parts();
    drop(checked.expect("checks"));
    let strong = lib.strong_count();
    assert_eq!(strong, 1, "only the warning's definition site holds lib");
    assert_eq!(
        named(warnings[0].location().definition()),
        at("lib.dsrv", "\"warn:alpha\"")
    );
}

#[test]
fn nodes_and_checked_forms_share_one_archive_without_holding_files() {
    let spec = program(
        &format!(
            "{HEADER}mod lib\nuse lib\nin x: Int\nout y: Int\ny = lib::good(x) + lib::good(x)\n"
        ),
        &[("lib", LIB)],
    );
    let lib = file(&spec, "lib.dsrv");
    let baseline = Arc::strong_count(&lib);

    let clone = spec.clone();
    let roots = (0..8)
        .map(|_| spec.var_expr(&VarName::new("y")).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        Arc::strong_count(&lib),
        baseline,
        "nodes hold IDs, not files"
    );

    let elaborated = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let checked_roots = (0..8)
        .map(|_| elaborated.var_expr(&VarName::new("y")).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        Arc::strong_count(&lib),
        baseline,
        "checking and elaborating share the archive"
    );
    assert!(AstShared::ptr_eq(
        elaborated.source().unchecked().sources(),
        elaborated.checked().unchecked().sources()
    ));
    assert!(AstShared::ptr_eq(
        clone.sources(),
        elaborated.source().unchecked().sources()
    ));
    drop((clone, roots, checked_roots, elaborated));
    assert_eq!(Arc::strong_count(&lib), 1, "only this test holds lib now");
}

#[test]
fn elaboration_keeps_every_nodes_origin() {
    let spec = program(
        &format!("{HEADER}mod lib\nuse lib\nin x: Int\nout y: Int\ny = lib::good(x) + x\n"),
        &[("lib", LIB)],
    );
    let elaborated = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let before = elaborated
        .source()
        .unchecked()
        .nodes()
        .map(|node| (node.span(), node.origin()))
        .collect::<Vec<_>>();
    let after = elaborated
        .checked()
        .unchecked()
        .nodes()
        .map(|node| (node.span(), node.origin()))
        .collect::<Vec<_>>();
    assert_eq!(before, after);
    assert!(after.iter().any(|(_, origin)| origin.definition.is_some()));
    assert!(after.iter().any(|(_, origin)| origin.definition.is_none()));
}

// -----------------------------------------------------------------------------
// Runtime-expression captures
// -----------------------------------------------------------------------------

const DYNAMIC_ROOT: &str = "use experimental::{modules, functions}\nmod lib\nmod types\nuse lib::*\n\
    in s: Str\nin x: Int\nout y: Int\ny = dynamic(s: Int)\n";

const TYPES: &str = "use experimental::{modules}\ntype Meters = Int\n";

fn prepared_site(
    elaborated: &crate::lang::dsrv::ElaboratedDsrvSpecification,
) -> RuntimeExpressionSite {
    elaborated
        .var_expr_ref(&VarName::new("y"))
        .unwrap()
        .postorder()
        .find(|node| crate::lang::dsrv::runtime_expression::is_runtime_expression(node.expr()))
        .expect("a dynamic node")
        .runtime_expression()
        .clone()
}

#[test]
fn a_site_captures_its_own_file_and_its_callable_files_only() {
    let spec = program(DYNAMIC_ROOT, &[("lib", LIB), ("types", TYPES)]);
    let types = file_probe(&spec, "types.dsrv");
    let elaborated = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let site = prepared_site(&elaborated);
    drop(elaborated);
    assert!(types.upgrade().is_none(), "no site needs the types module");
    let captured = site.sources().expect("a site of a program is located");
    let labels = captured
        .files()
        .map(|(_, file)| file.label().to_string())
        .collect::<Vec<_>>();
    assert_eq!(labels, ["root.dsrv", "lib.dsrv"]);
}

#[test]
fn runtime_text_errors_own_their_files_after_the_program_is_dropped() {
    let spec = program(DYNAMIC_ROOT, &[("lib", LIB), ("types", TYPES)]);
    let elaborated = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let site = prepared_site(&elaborated);
    drop(elaborated);

    let error = site
        .parse_and_check("bad(x)")
        .expect_err("bad does not check");
    let crate::lang::dsrv::runtime_expression::RuntimeExpressionError::TypeCheck { errors, .. } =
        error
    else {
        panic!("expected a type error, got {error:?}");
    };
    drop(site);
    let (primary, definition) = primary_and_definition(errors[0].location());
    assert_eq!(primary, at(RUNTIME_EXPRESSION_LABEL, "bad(x)"));
    let (label, text) = definition.expect("a definition note");
    assert_eq!(label, "lib.dsrv");
    assert!("n + true".contains(&text), "defined at {text:?}");

    // Text calling nothing is still located in itself.
    let spec = program(DYNAMIC_ROOT, &[("lib", LIB), ("types", TYPES)]);
    let elaborated = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let error = prepared_site(&elaborated)
        .parse_and_check("x + true")
        .expect_err("ill-typed");
    let crate::lang::dsrv::runtime_expression::RuntimeExpressionError::TypeCheck { errors, .. } =
        error
    else {
        panic!("expected a type error");
    };
    assert_eq!(
        named(errors[0].location().primary()).map(|(label, _)| label),
        Some(RUNTIME_EXPRESSION_LABEL.to_owned())
    );
    assert!(errors[0].location().definition().is_none());
}

#[test]
fn sites_nested_in_runtime_text_capture_that_text() {
    let spec = program(DYNAMIC_ROOT, &[("lib", LIB), ("types", TYPES)]);
    let lib = file_probe(&spec, "lib.dsrv");
    let elaborated = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let outer = prepared_site(&elaborated);
    drop(elaborated);
    let accepted = outer
        .parse_and_check("good(dynamic(s: Int))")
        .expect("nested text checks");
    drop(outer);
    let nested = accepted
        .as_ref()
        .postorder()
        .find(|node| crate::lang::dsrv::runtime_expression::is_runtime_expression(node.expr()))
        .expect("a nested dynamic")
        .runtime_expression()
        .clone();
    drop(accepted);
    let labels = nested
        .sources()
        .expect("nested sites are located")
        .files()
        .map(|(_, file)| file.label().to_string())
        .collect::<Vec<_>>();
    assert_eq!(labels, ["lib.dsrv", RUNTIME_EXPRESSION_LABEL]);
    let error = nested
        .parse_and_check("bad(1)")
        .expect_err("bad does not check");
    let crate::lang::dsrv::runtime_expression::RuntimeExpressionError::TypeCheck { errors, .. } =
        error
    else {
        panic!("expected a type error");
    };
    assert_eq!(
        named(errors[0].location().definition()).map(|(label, _)| label),
        Some("lib.dsrv".to_owned())
    );
    drop((nested, errors));
    assert!(lib.upgrade().is_none(), "no capture outlives its last site");
}

#[test]
fn a_site_is_equal_to_the_same_site_captured_elsewhere() {
    let spec = program(DYNAMIC_ROOT, &[("lib", LIB), ("types", TYPES)]);
    let elaborated = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let site = prepared_site(&elaborated);
    let uncaptured = site.without_sources();
    assert!(uncaptured.sources().is_none());
    assert_eq!(site, uncaptured);
    assert!(site.same_lexical_environment(&uncaptured));
}

// -----------------------------------------------------------------------------
// Identity
// -----------------------------------------------------------------------------

/// What the dataflow descriptor records of the `dynamic` node's site.
fn semantic_identity(spec: DsrvSpecification) -> String {
    let elaborated = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let site = prepared_site(&elaborated);
    let mut described = serde_json::to_string(site.context().fingerprint()).unwrap();
    site.callable().describe(&mut described);
    described
}

#[test]
fn relabelling_or_renumbering_files_keeps_semantic_identity() {
    let modules = [("lib", LIB), ("types", TYPES)];
    let original = semantic_identity(program(DYNAMIC_ROOT, &modules));
    let relabelled = semantic_identity(program_labelled(
        DYNAMIC_ROOT,
        "elsewhere/root.dsrv",
        &modules,
        "elsewhere/",
    ));
    assert_eq!(original, relabelled);
    // Declaring the modules in the other order numbers their files the
    // other way round.
    let reordered = semantic_identity(program(
        &DYNAMIC_ROOT.replace("mod lib\nmod types\n", "mod types\nmod lib\n"),
        &modules,
    ));
    assert_eq!(original, reordered);
    assert!(!original.contains("lib.dsrv"));

    let changed = semantic_identity(program(
        DYNAMIC_ROOT,
        &[("lib", &LIB.replace("n * 2", "n * 3")), ("types", TYPES)],
    ));
    assert_ne!(original, changed, "a changed def body is a different site");
}

#[test]
fn describing_a_parsed_body_ignores_its_origin() {
    let spec = program(
        &format!("{HEADER}mod mid\nuse mid::*\nin s: Str\nout y: Int\ny = dynamic(s: Int)\n"),
        &[
            (
                "mid",
                "use experimental::{modules, functions}\nmod leaf\nuse mid::leaf\n\
                 def relay(n: Int) -> Int = mid::leaf::twice(n)\n",
            ),
            (
                "mid::leaf",
                "use experimental::{modules, functions}\ndef twice(n: Int) -> Int = n * 2\n",
            ),
        ],
    );
    let described = semantic_identity(spec);
    assert!(!described.contains("definition"), "{described}");
    assert!(!described.contains("SourceId"), "{described}");
}

// -----------------------------------------------------------------------------
// Threads
// -----------------------------------------------------------------------------

static_assertions::assert_impl_all!(SourceFile: Send, Sync);
static_assertions::assert_impl_all!(OwnedSite: Send, Sync);
static_assertions::assert_impl_all!(SourceLocation: Send, Sync);
static_assertions::assert_impl_all!(SemanticError: Send, Sync);
static_assertions::assert_impl_all!(SemanticWarning: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(SourceArchive: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(RuntimeExpressionSite: Send, Sync);

#[test]
fn findings_render_on_another_thread_after_the_program_is_dropped() {
    let spec = program(WARNED_AND_FAILED, &[("lib", LIB), ("other", OTHER)]);
    let (result, warnings) = spec.check(TypeCheckOptions::STRICT).into_parts();
    let errors = result.expect_err("bad does not check");
    let rendered = std::thread::spawn(move || {
        (
            primary_and_definition(errors[0].location()),
            primary_and_definition(warnings[0].location()),
        )
    })
    .join()
    .unwrap();
    assert_eq!(rendered.0.0, at("root.dsrv", "lib::bad(x)"));
    assert_eq!(rendered.1.1, at("lib.dsrv", "\"warn:alpha\""));
}

#[cfg(feature = "thread-safe-ast")]
#[test]
fn a_prepared_site_accepts_text_on_another_thread_after_the_program_is_dropped() {
    let spec = program(DYNAMIC_ROOT, &[("lib", LIB), ("types", TYPES)]);
    let elaborated = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let site = prepared_site(&elaborated);
    drop(elaborated);
    let label = std::thread::spawn(move || {
        let error = site
            .parse_and_check("bad(x)")
            .expect_err("bad does not check");
        let crate::lang::dsrv::runtime_expression::RuntimeExpressionError::TypeCheck {
            errors, ..
        } = error
        else {
            panic!("expected a type error");
        };
        named(errors[0].location().definition()).map(|(label, _)| label)
    })
    .join()
    .unwrap();
    assert_eq!(label, Some("lib.dsrv".to_owned()));
}

#[test]
fn checked_specifications_of_labelled_text_are_located() {
    let report = CheckedDsrvSpecification::parse_with(
        "use experimental::{casts}\nin x: Int\nout y: Int\ny = x as Int",
        TypeCheckOptions::STRICT,
    )
    .unwrap();
    let (_, warnings) = report.into_parts();
    assert_eq!(
        named(warnings[0].location().primary()),
        at(STRING_LABEL, "x as Int")
    );
}
