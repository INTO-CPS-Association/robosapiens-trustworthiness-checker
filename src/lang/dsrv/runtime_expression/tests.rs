use std::panic::{AssertUnwindSafe, catch_unwind};

use super::*;
use crate::VarName;
use crate::dsrv_fixtures::checked;
use crate::lang::dsrv::ast::CheckedDsrvSpecification;
use crate::lang::dsrv::elaborate::ElaboratedDsrvSpecification;

/// Runtime expressions in a root equation, an auxiliary equation and a def
/// body, a restricted scope, and an equation with none.
const OCCURRENCES: &str = "use experimental::{functions}\n\
    def pick(s: Str) -> Int = dynamic(s: Int)\n\
    in source: Str\n\
    in x: Int\n\
    aux a: Int\n\
    out y: Int\n\
    out z: Int\n\
    out w: Int\n\
    a = defer(source: Int)\n\
    y = a + pick(source)\n\
    z = dynamic(source: Int, {x}) + x\n\
    w = x + 1";

fn elaborated(source: &str) -> ElaboratedDsrvSpecification {
    checked(source).elaborate()
}

fn var(name: &str) -> VarName {
    VarName::new(name)
}

/// Every `dynamic` and `defer` occurrence of `expr`.
fn occurrences(expr: &Expr) -> Vec<ExprRef<'_>> {
    expr.as_ref()
        .postorder()
        .filter(|node| matches!(node.kind(), ExprKind::Dynamic(..) | ExprKind::Defer(..)))
        .collect()
}

/// The only runtime-expression occurrence of `expr`.
fn occurrence(expr: &Expr) -> ExprRef<'_> {
    let [node] = occurrences(expr)[..] else {
        panic!("expected exactly one occurrence in {expr}");
    };
    node
}

fn site_count(spec: &CheckedDsrvSpecification) -> Option<usize> {
    spec.prepared_site_count()
}

#[test]
fn checked_inspection_stays_unprepared_and_elaboration_prepares_every_occurrence() {
    let checked = checked(OCCURRENCES);
    assert_eq!(site_count(&checked), None);

    let elaborated = checked.elaborate();
    assert_eq!(site_count(elaborated.source()), None);
    assert_eq!(site_count(elaborated.checked()), Some(3));
    for name in ["a", "y", "z"] {
        let expr = elaborated
            .var_expr(&var(name))
            .expect("the equation exists");
        let node = occurrence(expr.expr());
        let site = expr.cursor(node).runtime_expression();
        let typing = site.typing().expect("an elaborated site is typed");
        assert_eq!(typing.expected, TCType::Int, "{name}");
        assert!(AstShared::ptr_eq(
            &typing.environment,
            expr.as_ref().shared_type_environment()
        ));
        assert_eq!(
            site.context().fingerprint(),
            node.metadata().context.as_ref().unwrap().fingerprint()
        );
    }
    let w = elaborated.var_expr(&var("w")).expect("w exists");
    assert!(occurrences(w.expr()).is_empty());
    assert_eq!(w.prepared_site_count(), Some(3));
}

#[test]
fn a_prepared_artefact_without_runtime_expressions_is_ready_and_empty() {
    let checked = checked("in x: Int\nout y: Int\ny = x");
    assert_eq!(site_count(&checked), None);
    assert_eq!(
        checked.var_expr(&var("y")).unwrap().prepared_site_count(),
        None
    );

    let elaborated = checked.elaborate();
    assert_eq!(site_count(elaborated.checked()), Some(0));
    assert_eq!(
        elaborated
            .var_expr(&var("y"))
            .unwrap()
            .prepared_site_count(),
        Some(0)
    );
}

#[test]
#[should_panic(expected = "runtime-expression sites were not prepared")]
fn sourced_execution_cannot_use_an_unprepared_site() {
    let checked = checked("in source: Str\nout y: Int\ny = dynamic(source: Int)");
    let expression = checked.var_expr(&var("y")).expect("y exists");
    expression
        .cursor(occurrence(expression.expr()))
        .runtime_expression();
}

#[test]
#[should_panic(expected = "runtime-expression sites were not prepared")]
fn an_untyped_view_of_an_unprepared_expression_is_refused() {
    let checked = checked("in source: Str\nout y: Int\ny = dynamic(source: Int)");
    checked.var_expr(&var("y")).expect("y exists").untyped();
}

#[test]
fn lookup_rejects_an_equal_numbered_node_from_other_storage() {
    let source = "in source: Str\nout y: Int\ny = dynamic(source: Int)";
    let left = elaborated(source);
    let right = elaborated(source);
    let left = left.var_expr(&var("y")).expect("left y exists");
    let right = right.var_expr(&var("y")).expect("right y exists");
    let foreign = occurrence(right.expr());
    assert_eq!(foreign.id(), occurrence(left.expr()).id());

    let sites = left.untyped();
    let lookup = catch_unwind(AssertUnwindSafe(|| {
        sites.site(foreign);
    }));
    let message = lookup.expect_err("a foreign node must be refused");
    let message = message
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| message.downcast_ref::<&str>().copied())
        .expect("the invariant panic has a string message");
    assert!(
        message.contains("different expression storage"),
        "{message}"
    );
}

#[test]
fn extracted_handles_keep_their_sites_after_the_specification_drops() {
    let (expr, untyped) = {
        let elaborated = elaborated(OCCURRENCES);
        let expr = elaborated.var_expr(&var("z")).expect("z exists");
        let untyped = elaborated.var_expr(&var("y")).expect("y exists").untyped();
        (expr, untyped)
    };
    let node = occurrence(expr.expr());
    let accepted = expr
        .cursor(node)
        .runtime_expression()
        .parse_and_check("x + 1")
        .expect("source checks at the extracted site");
    assert_eq!(accepted.typ(), &TCType::Int);

    let node = occurrence(untyped.expr());
    assert!(untyped.site(node).parse_unchecked("x + 1").is_ok());
}

#[test]
fn sites_and_untyped_views_do_not_own_their_checked_context() {
    let elaborated = elaborated(OCCURRENCES);
    let expr = elaborated.var_expr(&var("z")).expect("z exists");
    drop(elaborated);
    let context = expr.downgrade_context();
    let site = expr
        .cursor(occurrence(expr.expr()))
        .runtime_expression()
        .clone();
    let untyped = expr.untyped();
    drop(expr);

    assert!(
        context.upgrade().is_none(),
        "neither a site nor an untyped view may keep its checked context alive"
    );
    assert!(site.parse_and_check("x").is_ok());
    assert!(untyped.site(occurrence(untyped.expr())).parse("x").is_ok());
}

#[test]
fn preparation_leaves_existing_handles_unchanged() {
    let checked = checked(OCCURRENCES);
    let prepared = checked.clone().prepare_sites();
    assert_eq!(site_count(&checked), None);
    assert_eq!(site_count(&prepared), Some(3));

    let unprepared = checked.var_expr(&var("z")).expect("z exists");
    let expression = unprepared.clone().prepare_sites();
    assert_eq!(unprepared.prepared_site_count(), None);
    assert_eq!(expression.prepared_site_count(), Some(1));
    assert!(!expression.shares_context_with(&unprepared));
}

#[test]
fn preparing_an_already_prepared_storage_keeps_its_sites() {
    let prepared = checked(OCCURRENCES).prepare_sites();
    let again = prepared.clone().prepare_sites();
    assert!(
        again
            .var_expr(&var("z"))
            .unwrap()
            .shares_context_with(&prepared.var_expr(&var("z")).unwrap())
    );
}

#[test]
fn elaboration_prepares_sites_for_its_new_storage() {
    let checked = checked(OCCURRENCES);
    let source = checked.clone().prepare_sites();
    let elaborated = checked.elaborate();
    let source = source.var_expr(&var("z")).expect("z exists");
    let target = elaborated.var_expr(&var("z")).expect("z exists");
    assert!(!source.expr().shares_storage_with(target.expr()));

    let sites = target.untyped();
    assert!(sites.site(occurrence(target.expr())).parse("x").is_ok());
    let stale = catch_unwind(AssertUnwindSafe(|| {
        sites.site(occurrence(source.expr()));
    }));
    assert!(
        stale.is_err(),
        "sites of the checked source must not be reused"
    );
}

#[test]
fn localisation_rebuilds_sites_and_drops_removed_occurrences() {
    let elaborated = elaborated(OCCURRENCES);
    let localised = elaborated
        .try_localise(&vec![var("y")])
        .expect("y localises");

    // The auxiliary `defer` is inlined into `y` beside the def body's
    // `dynamic`; `z`'s occurrence is gone with `z`.
    assert_eq!(site_count(localised.checked()), Some(2));
    assert_eq!(site_count(elaborated.checked()), Some(3));
    let y = localised.var_expr(&var("y")).expect("y survives");
    let nodes = occurrences(y.expr());
    assert_eq!(nodes.len(), 2);
    for node in nodes {
        let site = y.cursor(node).runtime_expression();
        assert_eq!(site.typing().unwrap().expected, TCType::Int);
        // Localisation keeps only the inputs `y` reads.
        assert!(site.parse_and_check("1").is_ok());
        assert!(site.parse_and_check("x").is_err());
    }

    let original = elaborated.var_expr(&var("y")).expect("y exists");
    assert!(!original.expr().shares_storage_with(y.expr()));
    assert!(
        original
            .cursor(occurrence(original.expr()))
            .runtime_expression()
            .parse_and_check("x")
            .is_ok()
    );
}

#[test]
fn an_untyped_view_keeps_the_lexical_site_and_ignores_types() {
    let elaborated = elaborated(OCCURRENCES);
    let expr = elaborated.var_expr(&var("z")).expect("z exists");
    let node = occurrence(expr.expr());
    let typed = expr.cursor(node).runtime_expression();
    let untyped = expr.untyped().site(node);
    assert!(typed.typing().is_some());
    assert!(untyped.typing().is_none());
    assert!(untyped.same_lexical_environment(typed));
    assert!(std::ptr::eq(untyped.context(), typed.context()));
    assert!(std::ptr::eq(untyped.callable(), typed.callable()));
}

#[test]
fn checked_runtime_source_is_prepared_before_it_escapes() {
    let elaborated = elaborated(OCCURRENCES);
    let expr = elaborated.var_expr(&var("z")).expect("z exists");
    let site = expr
        .cursor(occurrence(expr.expr()))
        .runtime_expression()
        .clone();
    drop(expr);
    drop(elaborated);

    let plain = site.parse_and_check("x + 1").expect("plain source checks");
    assert_eq!(plain.prepared_site_count(), Some(0));

    let nested = site
        .parse_and_check("defer(source: Int) + x")
        .expect("nested source checks");
    assert_eq!(nested.prepared_site_count(), Some(1));
    let nested_site = nested
        .cursor(occurrence(nested.expr()))
        .runtime_expression();
    assert_eq!(nested_site.typing().unwrap().expected, TCType::Int);
    assert!(nested_site.same_lexical_environment(&site));
}

#[test]
fn parse_only_source_prepares_untyped_sites() {
    let elaborated = elaborated(OCCURRENCES);
    let expr = elaborated.var_expr(&var("z")).expect("z exists");
    let site = expr
        .cursor(occurrence(expr.expr()))
        .runtime_expression()
        .clone();

    let parsed = site
        .parse_unchecked("dynamic(source: Int) + true")
        .expect("parse-only acceptance does not check");
    let nested = parsed.site(occurrence(parsed.expr()));
    assert!(nested.typing().is_none());
    assert!(nested.same_lexical_environment(&site));
    assert!(
        site.parse_and_check("dynamic(source: Int) + true").is_err(),
        "the same source is refused where it is checked"
    );
}

#[test]
fn runtime_source_may_call_a_def_visible_at_its_site() {
    let elaborated = elaborated(
        "use experimental::{functions}\n\
         def twice(n: Int) -> Int = n * 2\n\
         in source: Str\nin x: Int\nout y: Int\ny = dynamic(source: Int)",
    );
    let expr = elaborated.var_expr(&var("y")).expect("y exists");
    let site = expr.cursor(occurrence(expr.expr())).runtime_expression();
    let accepted = site.parse_and_check("twice(x)").expect("twice is visible");
    assert!(
        accepted.expr().to_string().contains('*'),
        "{}",
        accepted.expr()
    );
    let parsed = site.parse_unchecked("twice(x)").expect("twice is visible");
    assert!(parsed.expr().to_string().contains('*'), "{}", parsed.expr());
}
